package com.google.code.or.binlog.impl.event;

public class GtidEvent extends AbstractBinlogEventV4 {
	private static final long serialVersionUID = -7912557062572847202L;
	private final byte[] sourceId;
	private final long transactionId;

	public GtidEvent(byte[] sourceId, long transactionId) {
		this.sourceId = sourceId;
		this.transactionId = transactionId;
	}

	public byte[] getSourceId() {
		return sourceId;
	}

	public long getTransactionId() {
		return transactionId;
	}
}
