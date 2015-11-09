package com.google.code.or.binlog;

public interface BinlogProcessorCallback {
	void onEvents(BinlogEventV4 event, byte[] rawPacket, boolean validated);
	void onException(Exception e, byte[] rawPacket);
}
