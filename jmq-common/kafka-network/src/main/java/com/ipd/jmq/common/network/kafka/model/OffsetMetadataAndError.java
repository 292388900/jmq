package com.ipd.jmq.common.network.kafka.model;

import com.ipd.jmq.common.network.kafka.exception.ErrorCode;

/**
 * Created by zhangkepeng on 16-8-4.
 */
public class OffsetMetadataAndError {

    private static final OffsetMetadataAndError noOffset = new OffsetMetadataAndError(OffsetAndMetadata.INVALID_OFFSET, OffsetAndMetadata.NO_METADATA, ErrorCode.NO_ERROR);
    private static final OffsetMetadataAndError offsetsLoading = new OffsetMetadataAndError(OffsetAndMetadata.INVALID_OFFSET, OffsetAndMetadata.NO_METADATA, ErrorCode.OFFSETS_LOAD_INPROGRESS);
    private static final OffsetMetadataAndError notOffsetManagerForGroup = new OffsetMetadataAndError(OffsetAndMetadata.INVALID_OFFSET, OffsetAndMetadata.NO_METADATA, ErrorCode.NOT_COORDINATOR_FOR_CONSUMER);
    private static final OffsetMetadataAndError unknownTopicOrPartition = new OffsetMetadataAndError(OffsetAndMetadata.INVALID_OFFSET, OffsetAndMetadata.NO_METADATA, ErrorCode.UNKNOWN_TOPIC_OR_PARTITION);

    private long offset;
    private String metadata = OffsetAndMetadata.NO_METADATA;
    private short error = ErrorCode.NO_ERROR;

    public static final OffsetMetadataAndError GroupCoordinatorNotAvailable = new OffsetMetadataAndError(OffsetAndMetadata.INVALID_OFFSET, OffsetAndMetadata.NO_METADATA, ErrorCode.GROUP_COORDINATOR_NOT_AVAILABLE);

    public OffsetMetadataAndError(long offset, String metadata, short error) {

        this.offset = offset;
        this.metadata = metadata;
        this.error = error;
    }

    public long getOffset() {
        return offset;
    }

    public String getMetadata() {
        return metadata;
    }

    public short getError() {
        return error;
    }

    @Override
    public String toString() {
        return String.format("OffsetMetadataAndError[%d,%s,%d]",offset, metadata, error);
    }

}
