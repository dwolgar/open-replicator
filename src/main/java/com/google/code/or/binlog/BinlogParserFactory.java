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
package com.google.code.or.binlog;

import com.google.code.or.binlog.ext.XChecksum;
import com.google.code.or.binlog.impl.FileBasedBinlogParser;
import com.google.code.or.binlog.impl.ReplicationBasedBinlogParser;
import com.google.code.or.binlog.impl.ext.FileBasedBinlogParserExt;
import com.google.code.or.binlog.impl.ext.ReplicationBasedBinlogParserExt;
import com.google.code.or.binlog.impl.parser.DeleteRowsEventParser;
import com.google.code.or.binlog.impl.parser.DeleteRowsEventV2Parser;
import com.google.code.or.binlog.impl.parser.FormatDescriptionEventParser;
import com.google.code.or.binlog.impl.parser.IncidentEventParser;
import com.google.code.or.binlog.impl.parser.IntvarEventParser;
import com.google.code.or.binlog.impl.parser.QueryEventParser;
import com.google.code.or.binlog.impl.parser.RandEventParser;
import com.google.code.or.binlog.impl.parser.RotateEventParser;
import com.google.code.or.binlog.impl.parser.StopEventParser;
import com.google.code.or.binlog.impl.parser.TableMapEventParser;
import com.google.code.or.binlog.impl.parser.UpdateRowsEventParser;
import com.google.code.or.binlog.impl.parser.UpdateRowsEventV2Parser;
import com.google.code.or.binlog.impl.parser.UserVarEventParser;
import com.google.code.or.binlog.impl.parser.WriteRowsEventParser;
import com.google.code.or.binlog.impl.parser.WriteRowsEventV2Parser;
import com.google.code.or.binlog.impl.parser.XidEventParser;
import com.google.code.or.binlog.impl.parser.ext.DeleteRowsEventParserExt;
import com.google.code.or.binlog.impl.parser.ext.DeleteRowsEventV2ParserExt;
import com.google.code.or.binlog.impl.parser.ext.FormatDescriptionEventParserExt;
import com.google.code.or.binlog.impl.parser.ext.IncidentEventParserExt;
import com.google.code.or.binlog.impl.parser.ext.IntvarEventParserExt;
import com.google.code.or.binlog.impl.parser.ext.QueryEventParserExt;
import com.google.code.or.binlog.impl.parser.ext.RandEventParserExt;
import com.google.code.or.binlog.impl.parser.ext.RotateEventParserExt;
import com.google.code.or.binlog.impl.parser.ext.StopEventParserExt;
import com.google.code.or.binlog.impl.parser.ext.TableMapEventParserExt;
import com.google.code.or.binlog.impl.parser.ext.UpdateRowsEventParserExt;
import com.google.code.or.binlog.impl.parser.ext.UpdateRowsEventV2ParserExt;
import com.google.code.or.binlog.impl.parser.ext.UserVarEventParserExt;
import com.google.code.or.binlog.impl.parser.ext.WriteRowsEventParserExt;
import com.google.code.or.binlog.impl.parser.ext.WriteRowsEventV2ParserExt;
import com.google.code.or.binlog.impl.parser.ext.XidEventParserExt;
import com.google.code.or.net.Transport;

/**
 * @author baomingfeng
 * 
 */
public class BinlogParserFactory {

  public static ReplicationBasedBinlogParser createReplicationBinlogParser(Transport transport,
      String binlogFileName) {
    //
    final ReplicationBasedBinlogParser r = new ReplicationBasedBinlogParser();
    r.registgerEventParser(new StopEventParser());
    r.registgerEventParser(new RotateEventParser());
    r.registgerEventParser(new IntvarEventParser());
    r.registgerEventParser(new XidEventParser());
    r.registgerEventParser(new RandEventParser());
    r.registgerEventParser(new QueryEventParser());
    r.registgerEventParser(new UserVarEventParser());
    r.registgerEventParser(new IncidentEventParser());
    r.registgerEventParser(new TableMapEventParser());
    r.registgerEventParser(new WriteRowsEventParser());
    r.registgerEventParser(new UpdateRowsEventParser());
    r.registgerEventParser(new DeleteRowsEventParser());
    r.registgerEventParser(new WriteRowsEventV2Parser());
    r.registgerEventParser(new UpdateRowsEventV2Parser());
    r.registgerEventParser(new DeleteRowsEventV2Parser());
    r.registgerEventParser(new FormatDescriptionEventParser());
    //
    r.setTransport(transport);
    r.setBinlogFileName(binlogFileName);
    return r;
  }

  public static ReplicationBasedBinlogParserExt createReplicationBinlogParserExt(
      Transport transport, String binlogFileName, XChecksum checksum) {
    //
    final ReplicationBasedBinlogParserExt r = new ReplicationBasedBinlogParserExt();
    r.registgerEventParser(new StopEventParserExt(checksum));
    r.registgerEventParser(new RotateEventParserExt(checksum));
    r.registgerEventParser(new IntvarEventParserExt(checksum));
    r.registgerEventParser(new XidEventParserExt(checksum));
    r.registgerEventParser(new RandEventParserExt(checksum));
    r.registgerEventParser(new QueryEventParserExt(checksum));
    r.registgerEventParser(new UserVarEventParserExt(checksum));
    r.registgerEventParser(new IncidentEventParserExt(checksum));
    r.registgerEventParser(new TableMapEventParserExt(checksum));
    r.registgerEventParser(new WriteRowsEventParserExt(checksum));
    r.registgerEventParser(new UpdateRowsEventParserExt(checksum));
    r.registgerEventParser(new DeleteRowsEventParserExt(checksum));
    r.registgerEventParser(new WriteRowsEventV2ParserExt(checksum));
    r.registgerEventParser(new UpdateRowsEventV2ParserExt(checksum));
    r.registgerEventParser(new DeleteRowsEventV2ParserExt(checksum));
    r.registgerEventParser(new FormatDescriptionEventParserExt(checksum));
    //
    r.setTransport(transport);
    r.setBinlogFileName(binlogFileName);
    r.setChecksum(checksum);
    return r;
  }

  public static FileBasedBinlogParser createFileBinlogParser(long startPosition, long stopPosition,
      String binlogFileName, String binlogFilePath) {
    //
    final FileBasedBinlogParser r = new FileBasedBinlogParser();
    r.registgerEventParser(new StopEventParser());
    r.registgerEventParser(new RotateEventParser());
    r.registgerEventParser(new IntvarEventParser());
    r.registgerEventParser(new XidEventParser());
    r.registgerEventParser(new RandEventParser());
    r.registgerEventParser(new QueryEventParser());
    r.registgerEventParser(new UserVarEventParser());
    r.registgerEventParser(new IncidentEventParser());
    r.registgerEventParser(new TableMapEventParser());
    r.registgerEventParser(new WriteRowsEventParser());
    r.registgerEventParser(new UpdateRowsEventParser());
    r.registgerEventParser(new DeleteRowsEventParser());
    r.registgerEventParser(new WriteRowsEventV2Parser());
    r.registgerEventParser(new UpdateRowsEventV2Parser());
    r.registgerEventParser(new DeleteRowsEventV2Parser());
    r.registgerEventParser(new FormatDescriptionEventParser());
    //
    r.setStopPosition(stopPosition);
    r.setStartPosition(startPosition);
    r.setBinlogFileName(binlogFileName);
    r.setBinlogFilePath(binlogFilePath);
    return r;
  }

  // XXX
  public static FileBasedBinlogParserExt createFileBinlogParserExt(long startPosition,
      long stopPosition, String binlogFileName, String binlogFilePath, XChecksum checksum) {
    //
    final FileBasedBinlogParserExt r = new FileBasedBinlogParserExt();
    r.registgerEventParser(new StopEventParserExt(checksum));
    r.registgerEventParser(new RotateEventParserExt(checksum));
    r.registgerEventParser(new IntvarEventParserExt(checksum));
    r.registgerEventParser(new XidEventParserExt(checksum));
    r.registgerEventParser(new RandEventParserExt(checksum));
    r.registgerEventParser(new QueryEventParserExt(checksum));
    r.registgerEventParser(new UserVarEventParserExt(checksum));
    r.registgerEventParser(new IncidentEventParserExt(checksum));
    r.registgerEventParser(new TableMapEventParserExt(checksum));
    r.registgerEventParser(new WriteRowsEventParserExt(checksum));
    r.registgerEventParser(new UpdateRowsEventParserExt(checksum));
    r.registgerEventParser(new DeleteRowsEventParserExt(checksum));
    r.registgerEventParser(new WriteRowsEventV2ParserExt(checksum));
    r.registgerEventParser(new UpdateRowsEventV2ParserExt(checksum));
    r.registgerEventParser(new DeleteRowsEventV2ParserExt(checksum));
    r.registgerEventParser(new FormatDescriptionEventParserExt(checksum));
    //
    r.setStopPosition(stopPosition);
    r.setStartPosition(startPosition);
    r.setBinlogFileName(binlogFileName);
    r.setBinlogFilePath(binlogFilePath);
    return r;
  }
}
