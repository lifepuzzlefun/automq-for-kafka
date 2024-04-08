/*
 * Copyright 2024, AutoMQ CO.,LTD.
 *
 * Use of this software is governed by the Business Source License
 * included in the file BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

package com.automq.stream.s3;

import com.automq.stream.s3.metadata.ObjectUtils;
import com.automq.stream.s3.model.StreamRecordBatch;
import com.automq.stream.s3.objects.ObjectStreamRange;
import com.automq.stream.s3.operator.S3Operator;
import com.automq.stream.s3.operator.Writer;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.CompositeByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.util.ByteProcessor;
import org.apache.kafka.common.protocol.types.*;
import org.apache.kafka.common.record.DefaultRecordBatch;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.utils.Crc32C;
import org.apache.kafka.common.utils.Java;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.channels.GatheringByteChannel;
import java.nio.channels.ScatteringByteChannel;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.CompletableFuture;

import static com.automq.stream.s3.ByteBufAlloc.WRITE_DATA_BLOCK_HEADER;
import static com.automq.stream.s3.ByteBufAlloc.WRITE_FOOTER;
import static com.automq.stream.s3.ByteBufAlloc.WRITE_INDEX_BLOCK;
import static com.automq.stream.s3.DataBlockIndex.BLOCK_INDEX_SIZE;
import static com.automq.stream.s3.ObjectWriter.DataBlock.BLOCK_HEADER_SIZE;

/**
 * Write stream records to a single object.
 */
public interface ObjectWriter {

    byte DATA_BLOCK_MAGIC = 0x5A;
    // TODO: first n bit is the compressed flag
    byte DATA_BLOCK_DEFAULT_FLAG = 0x02;

    static ObjectWriter writer(long objectId, S3Operator s3Operator, int blockSizeThreshold, int partSizeThreshold) {
        return new DefaultObjectWriter(objectId, s3Operator, blockSizeThreshold, partSizeThreshold);
    }

    static ObjectWriter noop(long objectId) {
        return new NoopObjectWriter(objectId);
    }

    void write(long streamId, List<StreamRecordBatch> records);

    CompletableFuture<Void> close();

    List<ObjectStreamRange> getStreamRanges();

    long objectId();

    long size();

    class DefaultObjectWriter implements ObjectWriter {

        private final int blockSizeThreshold;
        private final int partSizeThreshold;
        private final List<DataBlock> waitingUploadBlocks;
        private final List<DataBlock> completedBlocks;
        private final Writer writer;
        private final long objectId;
        private int waitingUploadBlocksSize;
        private IndexBlock indexBlock;
        private long size;

        /**
         * Create a new object writer.
         *
         * @param objectId           object id
         * @param s3Operator         S3 operator
         * @param blockSizeThreshold the max size of a block
         * @param partSizeThreshold  the max size of a part. If it is smaller than {@link Writer#MIN_PART_SIZE}, it will be set to {@link Writer#MIN_PART_SIZE}.
         */
        public DefaultObjectWriter(long objectId, S3Operator s3Operator, int blockSizeThreshold,
            int partSizeThreshold) {
            this.objectId = objectId;
            String objectKey = ObjectUtils.genKey(0, objectId);
            this.blockSizeThreshold = blockSizeThreshold;
            this.partSizeThreshold = Math.max(Writer.MIN_PART_SIZE, partSizeThreshold);
            waitingUploadBlocks = new LinkedList<>();
            completedBlocks = new LinkedList<>();
            writer = s3Operator.writer(objectKey);
        }

        public void write(long streamId, List<StreamRecordBatch> records) {
            List<List<StreamRecordBatch>> blocks = groupByBlock(records);
            blocks.forEach(blockRecords -> {
                DataBlock block = new DataBlock(streamId, blockRecords);
                waitingUploadBlocks.add(block);
                waitingUploadBlocksSize += block.size();
            });
            if (waitingUploadBlocksSize >= partSizeThreshold) {
                tryUploadPart();
            }
        }

        private List<List<StreamRecordBatch>> groupByBlock(List<StreamRecordBatch> records) {
            List<List<StreamRecordBatch>> blocks = new LinkedList<>();
            List<StreamRecordBatch> blockRecords = new ArrayList<>(records.size());
            for (StreamRecordBatch record : records) {
                size += record.size();
                blockRecords.add(record);
                if (size >= blockSizeThreshold) {
                    blocks.add(blockRecords);
                    blockRecords = new ArrayList<>(records.size());
                    size = 0;
                }
            }
            if (!blockRecords.isEmpty()) {
                blocks.add(blockRecords);
            }
            return blocks;
        }

        private synchronized void tryUploadPart() {
            for (; ; ) {
                List<DataBlock> uploadBlocks = new ArrayList<>(waitingUploadBlocks.size());
                boolean partFull = false;
                int size = 0;
                for (DataBlock block : waitingUploadBlocks) {
                    uploadBlocks.add(block);
                    size += block.size();
                    if (size >= partSizeThreshold) {
                        partFull = true;
                        break;
                    }
                }
                if (partFull) {
                    CompositeByteBuf partBuf = ByteBufAlloc.compositeByteBuffer();
                    for (DataBlock block : uploadBlocks) {
                        waitingUploadBlocksSize -= block.size();
                        partBuf.addComponent(true, block.buffer());
                    }
                    writer.write(partBuf);
                    completedBlocks.addAll(uploadBlocks);
                    waitingUploadBlocks.removeIf(uploadBlocks::contains);
                } else {
                    break;
                }
            }
        }

        public CompletableFuture<Void> close() {
            CompositeByteBuf buf = ByteBufAlloc.compositeByteBuffer();
            for (DataBlock block : waitingUploadBlocks) {
                buf.addComponent(true, block.buffer());
                completedBlocks.add(block);
            }
            waitingUploadBlocks.clear();
            indexBlock = new IndexBlock();
            buf.addComponent(true, indexBlock.buffer());
            Footer footer = new Footer(indexBlock.position(), indexBlock.size());
            buf.addComponent(true, footer.buffer());
            writer.write(buf.duplicate());
            size = indexBlock.position() + indexBlock.size() + footer.size();
            return writer.close();
        }

        public List<ObjectStreamRange> getStreamRanges() {
            List<ObjectStreamRange> streamRanges = new LinkedList<>();
            ObjectStreamRange lastStreamRange = null;
            for (DataBlock block : completedBlocks) {
                ObjectStreamRange streamRange = block.getStreamRange();
                if (lastStreamRange == null || lastStreamRange.getStreamId() != streamRange.getStreamId()) {
                    if (lastStreamRange != null) {
                        streamRanges.add(lastStreamRange);
                    }
                    lastStreamRange = new ObjectStreamRange();
                    lastStreamRange.setStreamId(streamRange.getStreamId());
                    lastStreamRange.setEpoch(streamRange.getEpoch());
                    lastStreamRange.setStartOffset(streamRange.getStartOffset());
                }
                lastStreamRange.setEndOffset(streamRange.getEndOffset());
            }
            if (lastStreamRange != null) {
                streamRanges.add(lastStreamRange);
            }
            return streamRanges;
        }

        public long objectId() {
            return objectId;
        }

        public long size() {
            return size;
        }

        class IndexBlock {
            private final ByteBuf buf;
            private final long position;

            public IndexBlock() {
                long nextPosition = 0;
                int indexBlockSize = BLOCK_INDEX_SIZE * completedBlocks.size();
                buf = ByteBufAlloc.byteBuffer(indexBlockSize, WRITE_INDEX_BLOCK);
                for (DataBlock block : completedBlocks) {
                    ObjectStreamRange streamRange = block.getStreamRange();
                    new DataBlockIndex(streamRange.getStreamId(), streamRange.getStartOffset(), (int) (streamRange.getEndOffset() - streamRange.getStartOffset()),
                        block.recordCount(), nextPosition, block.size()).encode(buf);
                    nextPosition += block.size();
                }
                position = nextPosition;
            }

            public ByteBuf buffer() {
                return buf.duplicate();
            }

            public long position() {
                return position;
            }

            public int size() {
                return buf.readableBytes();
            }
        }
    }

    class DataBlock {
        public static final int BLOCK_HEADER_SIZE = 1 /* magic */ + 1/* flag */ + 4 /* record count*/ + 4 /* data length */;
        private final CompositeByteBuf encodedBuf;
        private final ObjectStreamRange streamRange;
        private final int recordCount;
        private final int size;

        public DataBlock(long streamId, List<StreamRecordBatch> records) {
            this.recordCount = records.size();
            this.encodedBuf = ByteBufAlloc.compositeByteBuffer();
            ByteBuf header = ByteBufAlloc.byteBuffer(BLOCK_HEADER_SIZE, WRITE_DATA_BLOCK_HEADER);
            header.writeByte(DATA_BLOCK_MAGIC);
            header.writeByte(DATA_BLOCK_DEFAULT_FLAG);
            header.writeInt(recordCount);
            header.writeInt(0); // data length
            encodedBuf.addComponent(true, header);
            records.forEach(r -> encodedBuf.addComponent(true, r.encoded().retain()));
            this.size = encodedBuf.readableBytes();
            encodedBuf.setInt(BLOCK_HEADER_SIZE - 4, size - BLOCK_HEADER_SIZE);
            this.streamRange = new ObjectStreamRange(streamId, records.get(0).getEpoch(), records.get(0).getBaseOffset(), records.get(records.size() - 1).getLastOffset(), size);
        }

        public int size() {
            return size;
        }

        public int recordCount() {
            return recordCount;
        }

        public ObjectStreamRange getStreamRange() {
            return streamRange;
        }

        public ByteBuf buffer() {
            return encodedBuf.duplicate();
        }
    }

    class Footer {
        public static final int FOOTER_SIZE = 48;
        private static final long MAGIC = 0x88e241b785f4cff7L;
        private final ByteBuf buf;

        public Footer(long indexStartPosition, int indexBlockLength) {
            buf = ByteBufAlloc.byteBuffer(FOOTER_SIZE, WRITE_FOOTER);
            // start position of index block
            buf.writeLong(indexStartPosition);
            // size of index block
            buf.writeInt(indexBlockLength);
            // reserved for future
            buf.writeZero(40 - 8 - 4);
            buf.writeLong(MAGIC);
        }

        public ByteBuf buffer() {
            return buf.duplicate();
        }

        public int size() {
            return FOOTER_SIZE;
        }

    }

    class NoopObjectWriter implements ObjectWriter {
        private final long objectId;

        public NoopObjectWriter(long objectId) {
            this.objectId = objectId;
        }

        @Override
        public void write(long streamId, List<StreamRecordBatch> records) {
        }

        @Override
        public CompletableFuture<Void> close() {
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public List<ObjectStreamRange> getStreamRanges() {
            return Collections.emptyList();
        }

        @Override
        public long objectId() {
            return objectId;
        }

        @Override
        public long size() {
            return 0;
        }
    }

    public static <T> String prettyPrintArrayList(List<T> list) {
        StringBuilder sb = new StringBuilder();
        sb.append("[\n");
        for (int i = 0; i < list.size(); i++) {
            sb.append(list.get(i));
            if (i < list.size() - 1) {
                sb.append(", \n");
            }
        }
        sb.append("\n]\n");
        return sb.toString();
    }

    public static void parseIndex(byte[] b, long sid, boolean verbose, boolean parseMemoryRecords) {
        ByteBuf byteBuf = Unpooled.wrappedBuffer(b);

        long idxBlockPos = byteBuf.getLong(b.length - Footer.FOOTER_SIZE);
        int idxBlockSize = byteBuf.getInt(b.length - Footer.FOOTER_SIZE + 8);
        long aLong = byteBuf.getLong(b.length - 8);

        if (verbose) {
            System.out.println("pos: " + idxBlockPos);
            System.out.println("blockSize: " + idxBlockSize);
            System.out.println(aLong == Footer.MAGIC);
            System.out.println();
        }

        ByteBuf indexBlock = byteBuf.slice((int)idxBlockPos, idxBlockSize);

        //     objectId=78, ranges=[3:4-6, 4:50390-55430, 5:8376-9384, ]

        long startPos = -1;
        int siZe = -1;
        for (int i = 0; i < 252 / BLOCK_INDEX_SIZE; i++) {
            long streamId = indexBlock.readLong();
            long startOffset = indexBlock.readLong();
            int endOffsetDelta = indexBlock.readInt();
            int recordCount = indexBlock.readInt();
            long startPosition = indexBlock.readLong();
            int size = indexBlock.readInt();

            if (verbose) {
                System.out.printf("streamId=%d, startOffset=%d, endOffset=%d, recordCount=%d, startPosition=%d, size=%d\n",
                        streamId, startOffset, startOffset + endOffsetDelta, recordCount, startPosition, size);
            }



            if (streamId == sid) {
                startPos = startPosition;
                siZe = size;
            }
        }


        ByteBuf slice = byteBuf.slice((int) startPos, siZe);

        byte magic = slice.readByte();
        byte flag = slice.readByte();
        int recordCount = slice.readInt();
        int dataLength = slice.readInt();

        if (verbose) {
            System.out.printf("magic=%08x, flag=%08x, recordCount=%d, length=%d\n\n",
                    magic, flag, recordCount, dataLength);
        }

        Charset cs = StandardCharsets.UTF_8;
        for (int i = 0; i < recordCount; i++) {
            StreamRecordBatch batch = StreamRecordBatchCodec.decode(slice);
            System.out.println("===========");
            System.out.println(batch);

            if (parseMemoryRecords) {
                MemoryRecords.readableRecords(batch.getPayload().nioBuffer()).batches().forEach(c -> {
                    System.out.printf("checksum=%d, baseOffset=%d, maxTimestamp=%d, timestampType=%s, " +
                                    "baseOffset=%d, lastOffset=%d, nextOffset=%d, magic=%d, producerId=%d, producerEpoch=%d, " +
                                    "baseSequence=%d, lastSequence=%d, compressionType=%s, sizeInBytes=%d, " +
                                    "partitionLeaderEpoch=%d, isControlBatch=%b, isTransactional=%b\n",
                            c.checksum(), c.baseOffset(), c.maxTimestamp(), c.timestampType().name(),
                            c.baseOffset(), c.lastOffset(), c.nextOffset(), c.magic(), c.producerId(), c.producerEpoch(),
                            c.baseSequence(), c.lastSequence(), c.compressionType().name(), c.sizeInBytes(),
                            c.partitionLeaderEpoch(), c.isControlBatch(), c.isTransactional());

                });
            }

            MetaKeyValue decode = MetaKeyValue.decode(batch.getPayload().nioBuffer());

            switch (decode.getKey()) {
                case "LEADER_EPOCH_CHECKPOINT":
                    System.out.printf("key=%s, value=%s\n", decode.getKey(), ElasticLeaderEpochCheckpointMeta.decode(decode.getValue()));
                    break;
                case "PARTITION":
                case "LOG":
                    System.out.printf("key=%s, value=%s\n", decode.getKey(), cs.decode(decode.getValue()));
                    break;
                case "PRODUCER_SNAPSHOTS":
                    System.out.printf("key=%s\n", decode.getKey());
                    Map<Long, ByteBuffer> snapshots = ElasticPartitionProducerSnapshotsMeta.decode(decode.getValue()).getSnapshots();
                    snapshots.forEach((key,content) -> {
                        try {
                            System.out.printf("offset=%d, content=%s\n", key, prettyPrintArrayList(ProducerStateManager.readSnapshot0(null, content.array())));
                        } catch (Exception e) {
                            throw new RuntimeException(e);
                        }
                    });



//                    ProducerStateManager.readSnapshot0(file, snapshots.get(offset).array())
                    break;


            }

            }

        System.out.println();
    }

    static void main(String[] args) throws IOException {
        byte[] b = Files.readAllBytes(new File("/tmp/68").toPath());
        parseIndex(b,4, true,true);

        byte[] b1 = Files.readAllBytes(new File("/tmp/77").toPath());
        parseIndex(b1,4, true,true);

        byte[] b2 = Files.readAllBytes(new File("/tmp/78").toPath());
        parseIndex(b2,3, false,false);

        byte[] b3 = Files.readAllBytes(new File("/tmp/97").toPath());
        parseIndex(b3, 3, false,false);

    }


}

class MetaKeyValue {
    public static final byte MAGIC_V0 = 0;

    private final String key;
    private final ByteBuffer value;

    private MetaKeyValue(String key, ByteBuffer value) {
        this.key = key;
        this.value = value;
    }

    public static MetaKeyValue of(String key, ByteBuffer value) {
        return new MetaKeyValue(key, value);
    }

    public static MetaKeyValue decode(ByteBuffer buf) throws IllegalArgumentException {
        // version, version = 0
        byte magic = buf.get();
        if (magic != MAGIC_V0) {
            throw new IllegalArgumentException("unsupported magic: " + magic);
        }
        // key, short
        int keyLength = buf.getInt();
        byte[] keyBytes = new byte[keyLength];
        buf.get(keyBytes);
        String key = new String(keyBytes, StandardCharsets.UTF_8);
        // value
        ByteBuffer value = buf.duplicate();
        return MetaKeyValue.of(key, value);
    }

    public static ByteBuffer encode(MetaKeyValue kv) {
        byte[] keyBytes = kv.key.getBytes(StandardCharsets.UTF_8);
        // MetaKeyValue encoded format =>
        //  magic => 1 byte
        // keyLength => 4 bytes
        //  value => bytes
        int length = 1 // magic length
                + 4 // key length
                + keyBytes.length // key payload
                + kv.value.remaining(); // value payload
        ByteBuf buf = Unpooled.buffer(length);
        buf.writeByte(MAGIC_V0);
        buf.writeInt(keyBytes.length);
        buf.writeBytes(keyBytes);
        buf.writeBytes(kv.value.duplicate());
        return buf.nioBuffer();
    }

    public String getKey() {
        return key;
    }

    public ByteBuffer getValue() {
        return value.duplicate();
    }
}

class EpochEntry {
    public final int epoch;
    public final long startOffset;

    public EpochEntry(int epoch, long startOffset) {
        this.epoch = epoch;
        this.startOffset = startOffset;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        EpochEntry that = (EpochEntry) o;
        return epoch == that.epoch && startOffset == that.startOffset;
    }

    @Override
    public int hashCode() {
        int result = epoch;
        result = 31 * result + Long.hashCode(startOffset);
        return result;
    }

    @Override
    public String toString() {
        return "EpochEntry(" +
                "epoch=" + epoch +
                ", startOffset=" + startOffset +
                ')';
    }
}


class ElasticLeaderEpochCheckpointMeta {
    private final int version;
    private List<EpochEntry> entries;

    public ElasticLeaderEpochCheckpointMeta(int version, List<EpochEntry> entries) {
        this.version = version;
        this.entries = entries;
    }

    public byte[] encode() {
        int totalLength = 4 // version
                + 4 // following entries size
                + 12 * entries.size(); // all entries
        ByteBuffer buffer = ByteBuffer.allocate(totalLength)
                .putInt(version)
                .putInt(entries.size());
        entries.forEach(entry -> buffer.putInt(entry.epoch).putLong(entry.startOffset));
        buffer.flip();
        return buffer.array();
    }

    public static ElasticLeaderEpochCheckpointMeta decode(ByteBuffer buffer) {
        int version = buffer.getInt();
        int entryCount = buffer.getInt();
        List<EpochEntry> entryList = new ArrayList<>(entryCount);
        while (buffer.hasRemaining()) {
            entryList.add(new EpochEntry(buffer.getInt(), buffer.getLong()));
        }
        if (entryList.size() != entryCount) {
            throw new RuntimeException("expect entry count:" + entryCount + ", decoded " + entryList.size() + " entries");
        }
        return new ElasticLeaderEpochCheckpointMeta(version, entryList);
    }

    public int version() {
        return version;
    }

    public List<EpochEntry> entries() {
        return entries;
    }

    public void setEntries(List<EpochEntry> entries) {
        this.entries = entries;
    }

    @Override
    public String toString() {
        return "ElasticLeaderEpochCheckpointMeta{" +
                "version=" + version +
                ", entries=" + entries +
                '}';
    }
}


class ElasticPartitionProducerSnapshotsMeta {
    public static final byte MAGIC_CODE = 0x18;
    private final Map<Long, ByteBuffer> snapshots;

    public ElasticPartitionProducerSnapshotsMeta() {
        this(new HashMap<>());
    }

    public ElasticPartitionProducerSnapshotsMeta(Map<Long, ByteBuffer> snapshots) {
        this.snapshots = snapshots;
    }

    public Map<Long, ByteBuffer> getSnapshots() {
        return snapshots;
    }

    public boolean isEmpty() {
        return snapshots.isEmpty();
    }

    public ByteBuffer encode() {
        int size = 1 /* magic code */ + snapshots.size() * (8 /* offset */ + 4 /* length */);
        for (ByteBuffer snapshot : snapshots.values()) {
            if (snapshot != null) {
                size += snapshot.remaining();
            }
        }
        ByteBuf buf = Unpooled.buffer(size);
        buf.writeByte(MAGIC_CODE);
        snapshots.forEach((offset, snapshot) -> {
            buf.writeLong(offset);
            buf.writeInt(snapshot.remaining());
            buf.writeBytes(snapshot.duplicate());
        });
        return buf.nioBuffer();
    }

    public static ElasticPartitionProducerSnapshotsMeta decode(ByteBuffer buffer) {
        ByteBuf buf = Unpooled.wrappedBuffer(buffer);
        byte magicCode = buf.readByte();
        if (magicCode != MAGIC_CODE) {
            throw new IllegalArgumentException("invalid magic code " + magicCode);
        }
        Map<Long, ByteBuffer> snapshots = new HashMap<>();
        while (buf.readableBytes() != 0) {
            long offset = buf.readLong();
            int length = buf.readInt();
            byte[] snapshot = new byte[length];
            buf.readBytes(snapshot);
            ByteBuffer snapshotBuf = ByteBuffer.wrap(snapshot);
            snapshots.put(offset, snapshotBuf);
        }
        return new ElasticPartitionProducerSnapshotsMeta(snapshots);
    }
}

class ProducerStateManager {

    public static final long LATE_TRANSACTION_BUFFER_MS = 5 * 60 * 1000;

    private static final short PRODUCER_SNAPSHOT_VERSION = 1;
    private static final String VERSION_FIELD = "version";
    private static final String CRC_FIELD = "crc";
    private static final String PRODUCER_ID_FIELD = "producer_id";
    private static final String LAST_SEQUENCE_FIELD = "last_sequence";
    private static final String PRODUCER_EPOCH_FIELD = "epoch";
    private static final String LAST_OFFSET_FIELD = "last_offset";
    private static final String OFFSET_DELTA_FIELD = "offset_delta";
    private static final String TIMESTAMP_FIELD = "timestamp";
    private static final String PRODUCER_ENTRIES_FIELD = "producer_entries";
    private static final String COORDINATOR_EPOCH_FIELD = "coordinator_epoch";
    private static final String CURRENT_TXN_FIRST_OFFSET_FIELD = "current_txn_first_offset";

    private static final int VERSION_OFFSET = 0;
    private static final int CRC_OFFSET = VERSION_OFFSET + 2;
    private static final int PRODUCER_ENTRIES_OFFSET = CRC_OFFSET + 4;

    private static final Schema PRODUCER_SNAPSHOT_ENTRY_SCHEMA =
            new Schema(new Field(PRODUCER_ID_FIELD, Type.INT64, "The producer ID"),
                    new Field(PRODUCER_EPOCH_FIELD, Type.INT16, "Current epoch of the producer"),
                    new Field(LAST_SEQUENCE_FIELD, Type.INT32, "Last written sequence of the producer"),
                    new Field(LAST_OFFSET_FIELD, Type.INT64, "Last written offset of the producer"),
                    new Field(OFFSET_DELTA_FIELD, Type.INT32, "The difference of the last sequence and first sequence in the last written batch"),
                    new Field(TIMESTAMP_FIELD, Type.INT64, "Max timestamp from the last written entry"),
                    new Field(COORDINATOR_EPOCH_FIELD, Type.INT32, "The epoch of the last transaction coordinator to send an end transaction marker"),
                    new Field(CURRENT_TXN_FIRST_OFFSET_FIELD, Type.INT64, "The first offset of the on-going transaction (-1 if there is none)"));
    private static final Schema PID_SNAPSHOT_MAP_SCHEMA =
            new Schema(new Field(VERSION_FIELD, Type.INT16, "Version of the snapshot file"),
                    new Field(CRC_FIELD, Type.UNSIGNED_INT32, "CRC of the snapshot data"),
                    new Field(PRODUCER_ENTRIES_FIELD, new ArrayOf(PRODUCER_SNAPSHOT_ENTRY_SCHEMA), "The entries in the producer table"));

    public static List<ProducerStateEntry> readSnapshot0(File file, byte[] buffer) throws Exception {
        try {
            // AutoMQ inject start
            if (buffer == null) {
                buffer = Files.readAllBytes(file.toPath());
            }
            // AutoMQ inject end
            Struct struct = PID_SNAPSHOT_MAP_SCHEMA.read(ByteBuffer.wrap(buffer));

            Short version = struct.getShort(VERSION_FIELD);
            if (version != PRODUCER_SNAPSHOT_VERSION)
                throw new Exception("Snapshot contained an unknown file version " + version);

            long crc = struct.getUnsignedInt(CRC_FIELD);
            long computedCrc = Crc32C.compute(buffer, PRODUCER_ENTRIES_OFFSET, buffer.length - PRODUCER_ENTRIES_OFFSET);
            if (crc != computedCrc)
                throw new Exception("Snapshot is corrupt (CRC is no longer valid). Stored crc: " + crc
                        + ". Computed crc: " + computedCrc);

            Object[] producerEntryFields = struct.getArray(PRODUCER_ENTRIES_FIELD);
            List<ProducerStateEntry> entries = new ArrayList<>(producerEntryFields.length);
            for (Object producerEntryObj : producerEntryFields) {
                Struct producerEntryStruct = (Struct) producerEntryObj;
                long producerId = producerEntryStruct.getLong(PRODUCER_ID_FIELD);
                short producerEpoch = producerEntryStruct.getShort(PRODUCER_EPOCH_FIELD);
                int seq = producerEntryStruct.getInt(LAST_SEQUENCE_FIELD);
                long offset = producerEntryStruct.getLong(LAST_OFFSET_FIELD);
                long timestamp = producerEntryStruct.getLong(TIMESTAMP_FIELD);
                int offsetDelta = producerEntryStruct.getInt(OFFSET_DELTA_FIELD);
                int coordinatorEpoch = producerEntryStruct.getInt(COORDINATOR_EPOCH_FIELD);
                long currentTxnFirstOffset = producerEntryStruct.getLong(CURRENT_TXN_FIRST_OFFSET_FIELD);

                OptionalLong currentTxnFirstOffsetVal = currentTxnFirstOffset >= 0 ? OptionalLong.of(currentTxnFirstOffset) : OptionalLong.empty();
                Optional<BatchMetadata> batchMetadata =
                        (offset >= 0) ? Optional.of(new BatchMetadata(seq, offset, offsetDelta, timestamp)) : Optional.empty();
                // AutoMQ inject start
                batchMetadata.ifPresent(m -> m.recovered = true);
                // AutoMQ inject end
                entries.add(new ProducerStateEntry(producerId, producerEpoch, coordinatorEpoch, timestamp, currentTxnFirstOffsetVal, batchMetadata));
            }

            return entries;
        } catch (Exception e) {
            throw new Exception("Snapshot failed schema validation: " + e.getMessage());
        }
    }


}


class ProducerStateEntry {
    public static final int NUM_BATCHES_TO_RETAIN = 5;
    private final long producerId;
    private final ArrayList<BatchMetadata> batchMetadata = new ArrayList<>();

    private short producerEpoch;
    private int coordinatorEpoch;
    private long lastTimestamp;
    private OptionalLong currentTxnFirstOffset;


    public ProducerStateEntry(long producerId, short producerEpoch, int coordinatorEpoch, long lastTimestamp, OptionalLong currentTxnFirstOffset, Optional<BatchMetadata> firstBatchMetadata) {
        this.producerId = producerId;
        this.producerEpoch = producerEpoch;
        this.coordinatorEpoch = coordinatorEpoch;
        this.lastTimestamp = lastTimestamp;
        this.currentTxnFirstOffset = currentTxnFirstOffset;
        firstBatchMetadata.ifPresent(batchMetadata::add);
    }

    public Collection<BatchMetadata> batchMetadata() {
        return Collections.unmodifiableCollection(batchMetadata);
    }

    public short producerEpoch() {
        return producerEpoch;
    }

    public long producerId() {
        return producerId;
    }

    public int coordinatorEpoch() {
        return coordinatorEpoch;
    }

    public long lastTimestamp() {
        return lastTimestamp;
    }

    public OptionalLong currentTxnFirstOffset() {
        return currentTxnFirstOffset;
    }

    public static <T> String prettyPrintArrayList(ArrayList<T> list) {
        StringBuilder sb = new StringBuilder();
        sb.append("[");
        for (int i = 0; i < list.size(); i++) {
            sb.append(list.get(i));
            if (i < list.size() - 1) {
                sb.append(", ");
            }
        }
        sb.append("]");
        return sb.toString();
    }

    @Override
    public String toString() {
        return "ProducerStateEntry(" +
                "producerId=" + producerId +
                ", producerEpoch=" + producerEpoch +
                ", currentTxnFirstOffset=" + currentTxnFirstOffset +
                ", coordinatorEpoch=" + coordinatorEpoch +
                ", lastTimestamp=" + lastTimestamp +
                ", batchMetadata=" + prettyPrintArrayList(batchMetadata) +
                ')';
    }
}

class BatchMetadata {

    public final int lastSeq;
    public final long lastOffset;
    public final int offsetDelta;
    public final long timestamp;

    // AutoMQ inject start
    public boolean recovered = false;
    // AutoMQ inject end

    public BatchMetadata(
            int lastSeq,
            long lastOffset,
            int offsetDelta,
            long timestamp) {
        this.lastSeq = lastSeq;
        this.lastOffset = lastOffset;
        this.offsetDelta = offsetDelta;
        this.timestamp = timestamp;
    }

    public int firstSeq() {
        return DefaultRecordBatch.decrementSequence(lastSeq, offsetDelta);
    }

    public long firstOffset() {
        return lastOffset - offsetDelta;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        BatchMetadata that = (BatchMetadata) o;

        return lastSeq == that.lastSeq &&
                lastOffset == that.lastOffset &&
                offsetDelta == that.offsetDelta &&
                timestamp == that.timestamp;
    }

    @Override
    public int hashCode() {
        int result = lastSeq;
        result = 31 * result + Long.hashCode(lastOffset);
        result = 31 * result + offsetDelta;
        result = 31 * result + Long.hashCode(timestamp);
        return result;
    }

    @Override
    public String toString() {
        return "BatchMetadata(" +
                "firstSeq=" + firstSeq() +
                ", lastSeq=" + lastSeq +
                ", firstOffset=" + firstOffset() +
                ", lastOffset=" + lastOffset +
                ", timestamp=" + timestamp +
                ')';
    }
}
