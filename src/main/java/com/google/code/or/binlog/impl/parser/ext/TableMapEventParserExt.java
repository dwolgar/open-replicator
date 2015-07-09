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
package com.google.code.or.binlog.impl.parser.ext;

import java.io.IOException;

import com.google.code.or.binlog.BinlogEventV4Header;
import com.google.code.or.binlog.BinlogParserContext;
import com.google.code.or.binlog.ext.XChecksum;
import com.google.code.or.binlog.impl.event.TableMapEvent;
import com.google.code.or.common.glossary.Metadata;
import com.google.code.or.io.XInputStream;

/**
 * 
 * @author Arbore
 */
public class TableMapEventParserExt extends AbstractBinlogEventParserExt {
	// 
	private boolean reusePreviousEvent = true;

	/**
	 * 
	 */
	public TableMapEventParserExt(XChecksum checksum) {
		super(TableMapEvent.EVENT_TYPE, checksum);
	}
	
	/**
	 * 
	 */
	public boolean isReusePreviousEvent() {
		return reusePreviousEvent;
	}

	public void setReusePreviousEvent(boolean reusePreviousEvent) {
		this.reusePreviousEvent = reusePreviousEvent;
	}
	
	/**
	 * 
	 */
	public void parse(XInputStream is, BinlogEventV4Header header, BinlogParserContext context)
	throws IOException {
		//
		final long tableId = is.readLong(6, checksum);
		if(this.reusePreviousEvent && context.getTableMapEvent(tableId) != null) {
			is.skip(is.available());//CRC32
			final TableMapEvent event = context.getTableMapEvent(tableId).copy();
			event.setHeader(header);
			checksum.reset();//CRC32
			context.getEventListener().onEvents(event);
			return;
		}
		
		//
		final TableMapEvent event = new TableMapEvent(header);
		event.setTableId(tableId);
		event.setReserved(is.readInt(2, checksum));
		event.setDatabaseNameLength(is.readInt(1, checksum));
		event.setDatabaseName(is.readNullTerminatedString(checksum));
		event.setTableNameLength(is.readInt(1, checksum));
		event.setTableName(is.readNullTerminatedString(checksum));
		event.setColumnCount(is.readUnsignedLong(checksum)); 
		event.setColumnTypes(is.readBytes(event.getColumnCount().intValue(), checksum));
		event.setColumnMetadataCount(is.readUnsignedLong(checksum)); 
		event.setColumnMetadata(Metadata.valueOf(event.getColumnTypes(), is.readBytes(event.getColumnMetadataCount().intValue(), checksum)));
		event.setColumnNullabilities(is.readBit(event.getColumnCount().intValue(), checksum));
		checksum.validateAndReset(is.readInt(4));//CRC32
		context.getEventListener().onEvents(event);
	}
}
