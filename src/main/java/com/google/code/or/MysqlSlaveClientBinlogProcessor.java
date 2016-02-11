package com.google.code.or;


import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.code.or.binlog.BinlogEventV4;
import com.google.code.or.binlog.BinlogProcessor;
import com.google.code.or.binlog.BinlogProcessorCallback;
import com.google.code.or.binlog.BinlogSimpleParser;
import com.google.code.or.binlog.ext.XChecksum;
import com.google.code.or.binlog.ext.XChecksumFactory;
import com.google.code.or.binlog.ext.XChecksum.ChecksumType;
import com.google.code.or.binlog.impl.BinlogSimpleParserImpl;
import com.google.code.or.common.glossary.column.StringColumn;
import com.google.code.or.io.XInputStream;
import com.google.code.or.io.impl.SocketFactoryImpl;
import com.google.code.or.io.util.XDeserializer;
import com.google.code.or.net.Packet;
import com.google.code.or.net.Transport;
import com.google.code.or.net.TransportException;
import com.google.code.or.net.impl.AuthenticatorImpl;
import com.google.code.or.net.impl.TransportImpl;
import com.google.code.or.net.impl.packet.EOFPacket;
import com.google.code.or.net.impl.packet.ErrorPacket;
import com.google.code.or.net.impl.packet.OKPacket;
import com.google.code.or.net.impl.packet.ResultSetFieldPacket;
import com.google.code.or.net.impl.packet.ResultSetHeaderPacket;
import com.google.code.or.net.impl.packet.ResultSetRowPacket;
import com.google.code.or.net.impl.packet.command.ComBinlogDumpPacket;
import com.google.code.or.net.impl.packet.command.ComPing;
import com.google.code.or.net.impl.packet.command.ComQuery;

public class MysqlSlaveClientBinlogProcessor implements BinlogProcessor {
	private static final Logger logger = LoggerFactory.getLogger(MysqlSlaveClientBinlogProcessor.class);
	
	private String masterHostname;
	private int masterPort;
	private String username;
	private String password;
	private int serverId;
	
	private int checkConnectionInterval;
	
	private String startBinlogFileName;
	private long startBinlogPosition;

	private String encoding;
	
	private Transport transport;

	private XChecksum checksum;
	private Map<String, String> variables;
	
	private BinlogSimpleParser parser;
	
	private ExecutorService pool;
	
	public MysqlSlaveClientBinlogProcessor() {
		this.encoding = "utf-8";
		this.variables = new HashMap<String, String>();
		this.parser = new BinlogSimpleParserImpl();
		this.checkConnectionInterval = 30;
	}
	
	
	private Transport getDefaultTransport() throws Exception {
		
		int level1BufferSize = 1024 * 1024;
		int level2BufferSize = 8 * 1024 * 1024;
		int socketReceiveBufferSize = 512 * 1024;

		//
		final TransportImpl transport = new TransportImpl();
		transport.setLevel1BufferSize(level1BufferSize);
		transport.setLevel2BufferSize(level2BufferSize);

		//
		final AuthenticatorImpl authenticator = new AuthenticatorImpl();
		authenticator.setUser(this.username);
		authenticator.setPassword(this.password);
		authenticator.setEncoding(this.encoding);
		transport.setAuthenticator(authenticator);

		//
		final SocketFactoryImpl socketFactory = new SocketFactoryImpl();
		socketFactory.setKeepAlive(true);
		socketFactory.setTcpNoDelay(false);
		socketFactory.setReceiveBufferSize(socketReceiveBufferSize);
		transport.setSocketFactory(socketFactory);
		return transport;
	}
	
	private void readSettings() throws Exception {
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
	private void bindSettings() throws Exception {
		// NONE | CRC32
		this.checksum = XChecksumFactory.create(variables.get("binlog_checksum"));

		if (this.checksum.getType() != ChecksumType.NONE) {
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
	private void dumpBinlog() throws Exception {
		//
		final ComBinlogDumpPacket command = new ComBinlogDumpPacket();
		// open-replicator-1.0.7: 0X00, alibaba canal:0X02, MySQL manual:// 0x01(BINLOG_DUMP_NON_BLOCK)
		command.setBinlogFlag(0x02);
		command.setServerId(this.serverId);
		command.setBinlogPosition(getStartBinlogPosition());
		command.setBinlogFileName(StringColumn.valueOf(getStartBinlogFileName().getBytes(this.encoding)));
		this.transport.getOutputStream().writePacket(command);
		this.transport.getOutputStream().flush();

		//
		final Packet packet = this.transport.getInputStream().readPacket();
		if (packet.getPacketBody()[0] == ErrorPacket.PACKET_MARKER) {
			final ErrorPacket error = ErrorPacket.valueOf(packet);
			throw new TransportException(error);
		}
	}

	private void startConnectionCheckThread() {
		if (this.pool == null) {
			this.pool = Executors.newSingleThreadExecutor();
		}
		
		logger.info("starting connection check thread [" + masterHostname + ":" + masterPort + "]");
		this.pool.submit(new Runnable() {

			@Override
			public void run() {
				while(true) {
					
					try {
						Thread.sleep(getCheckConnectionInterval());
					} catch(Exception e) {
					}
					
					logger.info("checking connection [" + masterHostname + ":" + masterPort + "]");
					
					try {
						transport.getOutputStream().writePacket(new ComPing());
						transport.getOutputStream().flush();
					} catch(Exception e) {
						try {
							transport.disconnect();
						} catch(Exception ex) {						
							
						}
						break;
					}
					
				}
					
			}
			
		});
	}
	
	@Override
	public XInputStream openInputStream() {
		try {
			if (this.transport == null) 
				this.transport = getDefaultTransport();

			this.transport.connect(this.masterHostname, this.masterPort);
			
			readSettings();

			bindSettings();

			dumpBinlog();
			
		    XInputStream is = this.transport.getInputStream();
		    
		    startConnectionCheckThread();
		    
		    return is;
		}
		catch (Exception ex) {
			logger.error("Error [" + ex.getMessage() + "]", ex);
			throw new RuntimeException(ex);
		}
	}
	
	@Override
	public void closeInputStream() {
		try {
			if (this.transport != null) {
				this.transport.disconnect();
			}
		}
		catch (Exception ex) {
			throw new RuntimeException(ex);
		}
	}
	
	@Override
	public void processOneEventRecord(XInputStream is, BinlogProcessorCallback callback) {
		XDeserializer stream = null;
		BinlogEventV4 event  = null;
		byte[] eventPacket   = null;
    	try {
    		final int packetLength = is.readInt(3);
    		final int packetSequence = is.readInt(1);
    		is.setReadLimit(packetLength); // Ensure the packet boundary

    		//
    		final int packetMarker = is.readInt(1);
    		if (packetMarker != OKPacket.PACKET_MARKER) { // 0x00
    			if ((byte) packetMarker == ErrorPacket.PACKET_MARKER) {
    				final ErrorPacket packet = ErrorPacket.valueOf(packetLength, packetSequence, packetMarker, is);
    				throw new RuntimeException(packet.toString());
    			} 
    			else if ((byte) packetMarker == EOFPacket.PACKET_MARKER) {
    				final EOFPacket packet = EOFPacket.valueOf(packetLength, packetSequence, packetMarker, is);
    				throw new RuntimeException(packet.toString());
    			} 
    			else {
    				throw new RuntimeException("assertion failed, invalid packet marker: " + packetMarker);
    			}
    		}
    		
    		boolean validated = true;
    		
    		if (this.checksum.getType() != ChecksumType.NONE) {
    			eventPacket = is.readBytes(packetLength - (1 + 4), checksum);
	    		
	    		try {
	    			checksum.validateAndReset(is.readInt(4));// CRC32
	    		}
	    		catch (Exception ex) {
	    			validated = false;
	    		}
    		}
    		else {
    			eventPacket = is.readBytes(packetLength - 1, checksum);
    		}
    		
    		is.setReadLimit(0);
 
    		if (eventPacket[eventPacket.length - 1] == -1) {
    			logger.debug("FOUND -1");
    			eventPacket[eventPacket.length - 1] = 0;
    		}
    		stream = new XDeserializer(eventPacket);
    		stream.setReadLimit(eventPacket.length);
    		event  = parser.parse(stream);
    		
    		callback.onEvents(event, eventPacket, validated);
    	
    	}
    	catch (Exception ex) {
    		logger.error("Error [" + ex.getMessage() + "]", ex);
    		callback.onException(ex, eventPacket);
    	}

	}
	


	public String getMasterHostname() {
		return masterHostname;
	}
	public void setMasterHostname(String masterHostname) {
		this.masterHostname = masterHostname;
	}

	public int getMasterPort() {
		return masterPort;
	}
	public void setMasterPort(int masterPort) {
		this.masterPort = masterPort;
	}

	public String getUsername() {
		return username;
	}
	public void setUsername(String username) {
		this.username = username;
	}

	public String getPassword() {
		return password;
	}
	public void setPassword(String password) {
		this.password = password;
	}

	public int getServerId() {
		return serverId;
	}
	public void setServerId(int serverId) {
		this.serverId = serverId;
	}

	public String getEncoding() {
		return encoding;
	}
	public void setEncoding(String encoding) {
		this.encoding = encoding;
	}


	public XChecksum getChecksum() {
		return checksum;
	}
	public void setChecksum(XChecksum checksum) {
		this.checksum = checksum;
	}

	public Map<String, String> getVariables() {
		return variables;
	}
	
	public String getStartBinlogFileName() {
		return startBinlogFileName;
	}
	@Override
	public void setStartBinlogFileName(String startBinlogFileName) {
		this.startBinlogFileName = startBinlogFileName;
	}


	public long getStartBinlogPosition() {
		return startBinlogPosition;
	}
	@Override
	public void setStartBinlogPosition(long startBinlogPosition) {
		this.startBinlogPosition = startBinlogPosition;
	}


	public BinlogSimpleParser getParser() {
		return parser;
	}
	public void setParser(BinlogSimpleParser parser) {
		this.parser = parser;
	}
	
	public int getCheckConnectionInterval() {
		return checkConnectionInterval * 1000;
	}
	public void setCheckConnectionInterval(int checkConnectionInterval) {
		this.checkConnectionInterval = checkConnectionInterval;
	}

	
	@Override
	public String toString() {
		return this.masterHostname + ":" + this.masterPort;
	}


}
