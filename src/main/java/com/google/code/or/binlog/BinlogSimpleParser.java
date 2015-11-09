package com.google.code.or.binlog;

import com.google.code.or.io.XInputStream;

public interface BinlogSimpleParser {
	public BinlogEventV4 parse(XInputStream inputStream);
}
