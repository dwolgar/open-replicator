/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.google.code.or.io;

import java.io.IOException;

import com.google.code.or.binlog.ext.XChecksum;
import com.google.code.or.common.glossary.UnsignedLong;
import com.google.code.or.common.glossary.column.BitColumn;
import com.google.code.or.common.glossary.column.StringColumn;

/**
 * 
 * @author Jingqi Xu
 */
public interface XInputStream {

	/**
	 * 
	 */
	void close() throws IOException;

	/**
	 * PLEASE PAY MORE ATTENTION when read checksum binlog events
	 */
	int available() throws IOException;

	/**
	 * PLEASE PAY MORE ATTENTION when read checksum binlog events
	 */
	boolean hasMore() throws IOException;

	void setReadLimit(int limit) throws IOException;

	/**
	 * 
	 */
	long skip(long n) throws IOException;

	/**
	 * @add
	 */
	long skip(long n, XChecksum checksum) throws IOException;

	int readInt(int length) throws IOException;

	/**
	 * @add
	 */
	int readInt(int length, XChecksum checksum) throws IOException;

	long readLong(int length) throws IOException;

	/**
	 * @add
	 */
	long readLong(int length, XChecksum checksum) throws IOException;

	byte[] readBytes(int length) throws IOException;

	/**
	 * @add
	 */
	byte[] readBytes(int length, XChecksum checksum) throws IOException;

	BitColumn readBit(int length) throws IOException;

	/**
	 * @add
	 */
	BitColumn readBit(int length, XChecksum checksum) throws IOException;

	int readSignedInt(int length) throws IOException;

	/**
	 * @add
	 */
	int readSignedInt(int length, XChecksum checksum) throws IOException;

	long readSignedLong(int length) throws IOException;

	/**
	 * @add
	 */
	long readSignedLong(int length, XChecksum checksum) throws IOException;

	UnsignedLong readUnsignedLong() throws IOException;

	/**
	 * @add
	 */
	UnsignedLong readUnsignedLong(XChecksum checksum) throws IOException;

	StringColumn readLengthCodedString() throws IOException;

	/**
	 * @add
	 */
	StringColumn readLengthCodedString(XChecksum checksum) throws IOException;

	StringColumn readNullTerminatedString() throws IOException;

	/**
	 * @add
	 */
	StringColumn readNullTerminatedString(XChecksum checksum) throws IOException;

	StringColumn readFixedLengthString(int length) throws IOException;

	/**
	 * @add
	 */
	StringColumn readFixedLengthString(int length, XChecksum checksum) throws IOException;

	int readInt(int length, boolean littleEndian) throws IOException;

	/**
	 * @add
	 */
	int readInt(int length, boolean littleEndian, XChecksum checksum) throws IOException;

	long readLong(int length, boolean littleEndian) throws IOException;

	/**
	 * @add
	 */
	long readLong(int length, boolean littleEndian, XChecksum checksum) throws IOException;

	BitColumn readBit(int length, boolean littleEndian) throws IOException;

	/**
	 * @add
	 */
	BitColumn readBit(int length, boolean littleEndian, XChecksum checksum) throws IOException;

	/**
	 * @add
	 */
	int read(XChecksum checksum) throws IOException;

	/**
	 * @add
	 */
	int read(final byte b[], final int off, final int len, XChecksum checksum) throws IOException;

}