package com.google.code.or.binlog;

import com.google.code.or.io.XInputStream;

public interface BinlogProcessor {
	public void setStartBinlogFileName(String startBinlogFileName);
	public void setStartBinlogPosition(long startBinlogPosition);

	public XInputStream openInputStream();
	public void closeInputStream();
	public void processOneEventRecord(XInputStream is, BinlogProcessorCallback callback);
}
