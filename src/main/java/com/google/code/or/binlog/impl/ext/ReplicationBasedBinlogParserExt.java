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
package com.google.code.or.binlog.impl.ext;

import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.code.or.binlog.BinlogEventParser;
import com.google.code.or.binlog.ext.XChecksum;
import com.google.code.or.binlog.ext.XChecksumNOPImpl;
import com.google.code.or.binlog.impl.ReplicationBasedBinlogParser;
import com.google.code.or.binlog.impl.event.BinlogEventV4HeaderImpl;
import com.google.code.or.io.XInputStream;
import com.google.code.or.net.impl.packet.EOFPacket;
import com.google.code.or.net.impl.packet.ErrorPacket;
import com.google.code.or.net.impl.packet.OKPacket;

/**
 * 
 * @author Jingqi Xu
 */
public class ReplicationBasedBinlogParserExt extends ReplicationBasedBinlogParser {
	//
	private static final Logger LOGGER = LoggerFactory.getLogger(ReplicationBasedBinlogParserExt.class);
	
	//@add by Arbore
	protected XChecksum checksum = new XChecksumNOPImpl();
	
	/**
	 * 
	 */
	public ReplicationBasedBinlogParserExt() {
	}

	public XChecksum getChecksum() {
		return this.checksum;
	}

	public void setChecksum(XChecksum checksum) {
		this.checksum = checksum;
	}
	
	@Override
	protected void doStart() throws Exception {
		this.checksum.reset();
	}

	@Override
	protected void doStop(long timeout, TimeUnit unit) throws Exception {
		this.checksum.reset();
	}

	/**
	 * 
	 */
	@Override
	protected void doParse() throws Exception {
		//
		final XInputStream is = this.transport.getInputStream();
		final Context context = new Context(this.binlogFileName);
		while(isRunning()) {
			try {
				// Parse packet
				final int packetLength = is.readInt(3);
				final int packetSequence = is.readInt(1);
				is.setReadLimit(packetLength); // Ensure the packet boundary
				
				//
				final int packetMarker = is.readInt(1);
				if(packetMarker != OKPacket.PACKET_MARKER) { // 0x00
					if((byte)packetMarker == ErrorPacket.PACKET_MARKER) {
						final ErrorPacket packet = ErrorPacket.valueOf(packetLength, packetSequence, packetMarker, is);
						throw new RuntimeException(packet.toString());
					} else if((byte)packetMarker == EOFPacket.PACKET_MARKER) {
						final EOFPacket packet = EOFPacket.valueOf(packetLength, packetSequence, packetMarker, is);
						throw new RuntimeException(packet.toString());
					} else {
						throw new RuntimeException("assertion failed, invalid packet marker: " + packetMarker);
					}
				}
				
				// Parse the event header
				final BinlogEventV4HeaderImpl header = new BinlogEventV4HeaderImpl();
				header.setTimestamp(is.readLong(4, checksum) * 1000L);
				header.setEventType(is.readInt(1, checksum));
				header.setServerId(is.readLong(4, checksum));
				header.setEventLength(is.readInt(4, checksum));
				header.setNextPosition(is.readLong(4, checksum));
				header.setFlags(is.readInt(2, checksum));
				header.setTimestampOfReceipt(System.currentTimeMillis());
				if(isVerbose() && LOGGER.isInfoEnabled()) {
					LOGGER.info("received an event, sequence: {}, header: {}", packetSequence, header);
				}
				
				// Parse the event body
				if(this.eventFilter != null && !this.eventFilter.accepts(header, context)) {
					this.defaultParser.parse(is, header, context);
				} else {
					BinlogEventParser parser = getEventParser(header.getEventType());
					if(parser == null) parser = this.defaultParser;
					parser.parse(is, header, context);
				}
				
				// Ensure the packet boundary
				if(is.available() != 0) {
					throw new RuntimeException("assertion failed, available: " + is.available() + ", event type: " + header.getEventType());
				}
			} finally {
				is.setReadLimit(0);
			}
		}
	}
}
