/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package com.google.code.or.io.util;

import java.io.ByteArrayInputStream;
import java.io.IOException;

import com.google.code.or.binlog.ext.XChecksum;
import com.google.code.or.common.glossary.UnsignedLong;
import com.google.code.or.common.glossary.column.BitColumn;
import com.google.code.or.common.glossary.column.StringColumn;
import com.google.code.or.io.XInputStream;
import com.google.code.or.io.impl.XInputStreamImpl;

/**
 * 
 * @author Jingqi Xu
 */
public class XDeserializer implements XInputStream {
  //
  private final XInputStream tis;

  /**
	 * 
	 */
  public XDeserializer(byte[] data) {
    this.tis = new XInputStreamImpl(new ByteArrayInputStream(data));
  }

  /**
	 * 
	 */
  public void close() throws IOException {
    this.tis.close();
  }

  /**
   * do not use the method where read checksum binlog event
   */
  public int available() throws IOException {
    return this.tis.available();
  }

  /**
   * do not use the method where read checksum binlog event
   */
  public boolean hasMore() throws IOException {
    return this.tis.hasMore();
  }

  public void setReadLimit(int limit) throws IOException {
    this.tis.setReadLimit(limit);
  }

  /**
	 * 
	 */
  public long skip(long n) throws IOException {
    return this.tis.skip(n);
  }

  @Override
  public long skip(long n, XChecksum checksum) throws IOException {
    return this.tis.skip(n, checksum);
  }

  public int readInt(int length) throws IOException {
    return this.tis.readInt(length);
  }

  @Override
  public int readInt(int length, XChecksum checksum) throws IOException {
    return this.tis.readInt(length, checksum);
  }

  public long readLong(int length) throws IOException {
    return this.tis.readLong(length);
  }

  @Override
  public long readLong(int length, XChecksum checksum) throws IOException {
    return this.tis.readLong(length, checksum);
  }

  public byte[] readBytes(int length) throws IOException {
    return this.tis.readBytes(length);
  }

  @Override
  public byte[] readBytes(int length, XChecksum checksum) throws IOException {
    return this.tis.readBytes(length, checksum);
  }

  public BitColumn readBit(int length) throws IOException {
    return this.tis.readBit(length);
  }

  @Override
  public BitColumn readBit(int length, XChecksum checksum) throws IOException {
    return this.tis.readBit(length, checksum);
  }

  public int readSignedInt(int length) throws IOException {
    return this.tis.readSignedInt(length);
  }

  @Override
  public int readSignedInt(int length, XChecksum checksum) throws IOException {
    return this.tis.readSignedInt(length, checksum);
  }

  public long readSignedLong(int length) throws IOException {
    return this.tis.readSignedLong(length);
  }

  @Override
  public long readSignedLong(int length, XChecksum checksum) throws IOException {
    return this.tis.readSignedLong(length, checksum);
  }

  public UnsignedLong readUnsignedLong() throws IOException {
    return this.tis.readUnsignedLong();
  }

  @Override
  public UnsignedLong readUnsignedLong(XChecksum checksum) throws IOException {
    return this.tis.readUnsignedLong(checksum);
  }

  public StringColumn readLengthCodedString() throws IOException {
    return this.tis.readLengthCodedString();
  }

  @Override
  public StringColumn readLengthCodedString(XChecksum checksum) throws IOException {
    return this.tis.readLengthCodedString(checksum);
  }

  public StringColumn readNullTerminatedString() throws IOException {
    return this.tis.readNullTerminatedString();
  }

  @Override
  public StringColumn readNullTerminatedString(XChecksum checksum) throws IOException {
    return this.tis.readNullTerminatedString(checksum);
  }

  public StringColumn readFixedLengthString(int length) throws IOException {
    return this.tis.readFixedLengthString(length);
  }

  @Override
  public StringColumn readFixedLengthString(int length, XChecksum checksum) throws IOException {
    return this.tis.readFixedLengthString(length, checksum);
  }

  public int readInt(int length, boolean littleEndian) throws IOException {
    return this.tis.readInt(length, littleEndian);
  }

  @Override
  public int readInt(int length, boolean littleEndian, XChecksum checksum) throws IOException {
    return this.tis.readInt(length, littleEndian, checksum);
  }

  public long readLong(int length, boolean littleEndian) throws IOException {
    return this.tis.readLong(length, littleEndian);
  }

  @Override
  public long readLong(int length, boolean littleEndian, XChecksum checksum) throws IOException {
    return this.tis.readLong(length, littleEndian, checksum);
  }

  public BitColumn readBit(int length, boolean littleEndian) throws IOException {
    return this.tis.readBit(length, littleEndian);
  }

  @Override
  public BitColumn readBit(int length, boolean littleEndian, XChecksum checksum) throws IOException {
    return this.tis.readBit(length, littleEndian, checksum);
  }

  @Override
  public int read(XChecksum checksum) throws IOException {
    return this.tis.read(checksum);
  }

  @Override
  public int read(byte[] b, int off, int len, XChecksum checksum) throws IOException {
    return this.tis.read(b, off, len, checksum);
  }
}
