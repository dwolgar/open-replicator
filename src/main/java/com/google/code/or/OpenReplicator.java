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
package com.google.code.or;

import java.util.HashMap;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import com.google.code.or.binlog.BinlogEventListener;
import com.google.code.or.binlog.BinlogParser;
import com.google.code.or.binlog.BinlogParserFactory;
import com.google.code.or.binlog.BinlogParserListener;
import com.google.code.or.binlog.ext.XChecksum;
import com.google.code.or.binlog.ext.XChecksum.ChecksumType;
import com.google.code.or.binlog.ext.XChecksumFactory;
import com.google.code.or.binlog.impl.ReplicationBasedBinlogParser;
import com.google.code.or.common.glossary.column.StringColumn;
import com.google.code.or.io.impl.SocketFactoryImpl;
import com.google.code.or.net.Packet;
import com.google.code.or.net.Transport;
import com.google.code.or.net.TransportException;
import com.google.code.or.net.impl.AuthenticatorImpl;
import com.google.code.or.net.impl.TransportImpl;
import com.google.code.or.net.impl.packet.EOFPacket;
import com.google.code.or.net.impl.packet.ErrorPacket;
import com.google.code.or.net.impl.packet.ResultSetFieldPacket;
import com.google.code.or.net.impl.packet.ResultSetHeaderPacket;
import com.google.code.or.net.impl.packet.ResultSetRowPacket;
import com.google.code.or.net.impl.packet.command.ComBinlogDumpPacket;
import com.google.code.or.net.impl.packet.command.ComQuery;

/**
 * 
 * @author Jingqi Xu
 */
public class OpenReplicator {
	//
	protected int port = 3306;
	protected String host;
	protected String user;
	protected String password;
	protected int serverId = 6789;
	protected String binlogFileName;
	protected long binlogPosition = 4;
	protected String encoding = "utf-8";
	protected int level1BufferSize = 1024 * 1024;
	protected int level2BufferSize = 8 * 1024 * 1024;
	protected int socketReceiveBufferSize = 512 * 1024;

	//
	protected Transport transport;
	protected BinlogParser binlogParser;
	protected BinlogEventListener binlogEventListener;
	protected final AtomicBoolean running = new AtomicBoolean(false);

	protected XChecksum checksum;

	// VARBINARY -> VARBINARY : StringColumn -> StringColumn
	protected HashMap<String, String> variables = new HashMap<String, String>();

	/**
	 * 
	 */
	public boolean isRunning() {
		return this.running.get();
	}

	public void start() throws Exception {
		//
		if (!this.running.compareAndSet(false, true)) {
			return;
		}

		//
		if (this.transport == null)
			this.transport = getDefaultTransport();
		this.transport.connect(this.host, this.port);

		//
		readSettings();

		//
		bindSettings();

		//
		dumpBinlog();

		//
		if (this.binlogParser == null)
			this.binlogParser = getDefaultBinlogParser();
		this.binlogParser.setEventListener(this.binlogEventListener);
		this.binlogParser.addParserListener(new BinlogParserListener.Adapter() {
			@Override
			public void onStop(BinlogParser parser) {
				stopQuietly(0, TimeUnit.MILLISECONDS);
			}
		});
		this.binlogParser.start();
	}

	public void stop(long timeout, TimeUnit unit) throws Exception {
		//
		if (!this.running.compareAndSet(true, false)) {
			return;
		}

		//
		this.transport.disconnect();
		this.binlogParser.stop(timeout, unit);
	}

	public void stopQuietly(long timeout, TimeUnit unit) {
		try {
			stop(timeout, unit);
		} catch (Exception e) {
			// NOP
		}
	}

	/**
	 * 
	 */
	public int getPort() {
		return port;
	}

	public void setPort(int port) {
		this.port = port;
	}

	public String getHost() {
		return host;
	}

	public void setHost(String host) {
		this.host = host;
	}

	public String getUser() {
		return user;
	}

	public void setUser(String user) {
		this.user = user;
	}

	public String getPassword() {
		return password;
	}

	public void setPassword(String password) {
		this.password = password;
	}

	public String getEncoding() {
		return encoding;
	}

	public void setEncoding(String encoding) {
		this.encoding = encoding;
	}

	public int getServerId() {
		return serverId;
	}

	public void setServerId(int serverId) {
		this.serverId = serverId;
	}

	public long getBinlogPosition() {
		return binlogPosition;
	}

	public void setBinlogPosition(long binlogPosition) {
		this.binlogPosition = binlogPosition;
	}

	public String getBinlogFileName() {
		return binlogFileName;
	}

	public void setBinlogFileName(String binlogFileName) {
		this.binlogFileName = binlogFileName;
	}

	public int getLevel1BufferSize() {
		return level1BufferSize;
	}

	public void setLevel1BufferSize(int level1BufferSize) {
		this.level1BufferSize = level1BufferSize;
	}

	public int getLevel2BufferSize() {
		return level2BufferSize;
	}

	public void setLevel2BufferSize(int level2BufferSize) {
		this.level2BufferSize = level2BufferSize;
	}

	public int getSocketReceiveBufferSize() {
		return socketReceiveBufferSize;
	}

	public void setSocketReceiveBufferSize(int socketReceiveBufferSize) {
		this.socketReceiveBufferSize = socketReceiveBufferSize;
	}

	/**
	 * 
	 */
	public Transport getTransport() {
		return transport;
	}

	public void setTransport(Transport transport) {
		this.transport = transport;
	}

	public BinlogParser getBinlogParser() {
		return binlogParser;
	}

	public void setBinlogParser(BinlogParser parser) {
		this.binlogParser = parser;
	}

	public BinlogEventListener getBinlogEventListener() {
		return binlogEventListener;
	}

	public void setBinlogEventListener(BinlogEventListener listener) {
		this.binlogEventListener = listener;
	}

	public XChecksum getChecksum() {
		return checksum;
	}

	public void setChecksum(XChecksum checksum) {
		this.checksum = checksum;
	}

	public HashMap<String, String> getVariables() {
		return this.variables;
	}

	protected void readSettings() throws Exception {
		//
		final ComQuery command = new ComQuery();
		String cmd = "SHOW GLOBAL VARIABLES";
		StringColumn sc = StringColumn.valueOf(cmd.getBytes("UTF-8"));
		command.setSql(sc);
		this.transport.getOutputStream().writePacket(command);
		this.transport.getOutputStream().flush();

		//
		Packet packet = transport.getInputStream().readPacket();
		if (packet.getPacketBody()[0] == ErrorPacket.PACKET_MARKER) {
			final ErrorPacket error = ErrorPacket.valueOf(packet);// Consume
			throw new TransportException(error);
		}

		ResultSetHeaderPacket.valueOf(packet);
		//
		while (true) {
			packet = transport.getInputStream().readPacket();
			if (packet.getPacketBody()[0] == EOFPacket.PACKET_MARKER) {
				EOFPacket.valueOf(packet);// Consume
				break;
			} else {
				ResultSetFieldPacket.valueOf(packet);
			}
		}

		//
		while (true) {
			packet = transport.getInputStream().readPacket();
			if (packet.getPacketBody()[0] == EOFPacket.PACKET_MARKER) {
				EOFPacket.valueOf(packet);// Consume
				break;
			} else {
				ResultSetRowPacket row = ResultSetRowPacket.valueOf(packet);
				List<StringColumn> entry = row.getColumns();
				// Lower Case Key
				String configKey = entry.get(0).toString().toLowerCase();
				// Upper Case Value
				String configVal = entry.get(1).toString().toUpperCase();
				variables.put(configKey, configVal);
			}
		}
	}

	/**
	 * 
	 */
	protected void bindSettings() throws Exception {
		// NONE | CRC32
		this.checksum = XChecksumFactory.create(variables.get("binlog_checksum"));

		if (checksum != null) {
			final ComQuery command = new ComQuery();
			String cmd = "SET @master_binlog_checksum= '@@global.binlog_checksum'";
			StringColumn sc = StringColumn.valueOf(cmd.getBytes("UTF-8"));
			command.setSql(sc);
			this.transport.getOutputStream().writePacket(command);
			this.transport.getOutputStream().flush();

			//
			final Packet packet = this.transport.getInputStream().readPacket();
			if (packet.getPacketBody()[0] == ErrorPacket.PACKET_MARKER) {
				final ErrorPacket error = ErrorPacket.valueOf(packet);
				throw new TransportException(error);
			}
		}
	}

	/**
	 * 
	 */
	protected void dumpBinlog() throws Exception {
		//
		final ComBinlogDumpPacket command = new ComBinlogDumpPacket();
		command.setBinlogFlag(0x02);//  open-replicator-1.0.7: 0X00, alibaba canal:0X02, MySQL manual: 0x01(BINLOG_DUMP_NON_BLOCK)
		command.setServerId(this.serverId);
		command.setBinlogPosition(this.binlogPosition);
		command.setBinlogFileName(StringColumn.valueOf(this.binlogFileName.getBytes(this.encoding)));
		this.transport.getOutputStream().writePacket(command);
		this.transport.getOutputStream().flush();

		//
		final Packet packet = this.transport.getInputStream().readPacket();
		if (packet.getPacketBody()[0] == ErrorPacket.PACKET_MARKER) {
			final ErrorPacket error = ErrorPacket.valueOf(packet);
			throw new TransportException(error);
		}
	}

	protected Transport getDefaultTransport() throws Exception {
		//
		final TransportImpl r = new TransportImpl();
		r.setLevel1BufferSize(this.level1BufferSize);
		r.setLevel2BufferSize(this.level2BufferSize);

		//
		final AuthenticatorImpl authenticator = new AuthenticatorImpl();
		authenticator.setUser(this.user);
		authenticator.setPassword(this.password);
		authenticator.setEncoding(this.encoding);
		r.setAuthenticator(authenticator);

		//
		final SocketFactoryImpl socketFactory = new SocketFactoryImpl();
		socketFactory.setKeepAlive(true);
		socketFactory.setTcpNoDelay(false);
		socketFactory.setReceiveBufferSize(this.socketReceiveBufferSize);
		r.setSocketFactory(socketFactory);
		return r;
	}

	protected ReplicationBasedBinlogParser getDefaultBinlogParser() throws Exception {
		if (checksum.getType() == ChecksumType.NONE)
			return BinlogParserFactory.createReplicationBinlogParser(this.transport,
					this.binlogFileName);

		return BinlogParserFactory.createReplicationBinlogParserExt(this.transport,
				this.binlogFileName, this.checksum);
	}
}
