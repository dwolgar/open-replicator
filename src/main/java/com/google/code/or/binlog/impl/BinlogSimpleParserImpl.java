package com.google.code.or.binlog.impl;

import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.code.or.binlog.BinlogEventListener;
import com.google.code.or.binlog.BinlogEventParser;
import com.google.code.or.binlog.BinlogEventV4;
import com.google.code.or.binlog.BinlogParserContext;
import com.google.code.or.binlog.BinlogSimpleParser;
import com.google.code.or.binlog.impl.event.BinlogEventV4HeaderImpl;
import com.google.code.or.binlog.impl.event.RotateEvent;
import com.google.code.or.binlog.impl.event.TableMapEvent;
import com.google.code.or.binlog.impl.parser.DeleteRowsEventParser;
import com.google.code.or.binlog.impl.parser.DeleteRowsEventV2Parser;
import com.google.code.or.binlog.impl.parser.FormatDescriptionEventParser;
import com.google.code.or.binlog.impl.parser.IncidentEventParser;
import com.google.code.or.binlog.impl.parser.IntvarEventParser;
import com.google.code.or.binlog.impl.parser.NopEventParser;
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
import com.google.code.or.io.XInputStream;

public class BinlogSimpleParserImpl implements BinlogSimpleParser, BinlogEventListener, BinlogParserContext {
	private final static Logger logger = LoggerFactory.getLogger(BinlogSimpleParserImpl.class);
	
	private final BinlogEventParser defaultParser;
	private final BinlogEventParser[] parsers;
	
	private final Map<Long, TableMapEvent> tableMapEvents;
	private String binlogFileName;
	private BinlogEventV4 parsedEvent;
	
	private boolean clearTableMapOnRotate;

	public BinlogSimpleParserImpl() {
		this.defaultParser = new NopEventParser();
		this.parsers = new BinlogEventParser[128];
		this.tableMapEvents = new HashMap<Long, TableMapEvent>();
		this.clearTableMapOnRotate = true;
		
	    this.registgerEventParser(new StopEventParser());
	    this.registgerEventParser(new RotateEventParser());
	    this.registgerEventParser(new IntvarEventParser());
	    this.registgerEventParser(new XidEventParser());
	    this.registgerEventParser(new RandEventParser());
	    this.registgerEventParser(new QueryEventParser());
	    this.registgerEventParser(new UserVarEventParser());
	    this.registgerEventParser(new IncidentEventParser());
	    this.registgerEventParser(new TableMapEventParser());
	    this.registgerEventParser(new WriteRowsEventParser());
	    this.registgerEventParser(new UpdateRowsEventParser());
	    this.registgerEventParser(new DeleteRowsEventParser());
	    this.registgerEventParser(new WriteRowsEventV2Parser());
	    this.registgerEventParser(new UpdateRowsEventV2Parser());
	    this.registgerEventParser(new DeleteRowsEventV2Parser());
	    this.registgerEventParser(new FormatDescriptionEventParser());
	}
	
	@Override
	public BinlogEventV4 parse(XInputStream inputStream) {
		try {
			final BinlogEventV4HeaderImpl header = new BinlogEventV4HeaderImpl();
			header.setTimestamp(inputStream.readLong(4) * 1000L);
			header.setEventType(inputStream.readInt(1));
			header.setServerId(inputStream.readLong(4));
			header.setEventLength(inputStream.readInt(4));
			header.setNextPosition(inputStream.readLong(4));
			header.setFlags(inputStream.readInt(2));
			header.setTimestampOfReceipt(System.currentTimeMillis());
			
			BinlogEventParser parser = getEventParser(header.getEventType());
			if (parser == null) 
				parser = this.defaultParser;
			
			parser.parse(inputStream, header, this);
			
			return this.parsedEvent;

		}
		catch (Exception ex) {
			logger.error("Error [" + ex.getMessage() + "]", ex);
			throw new RuntimeException(ex);
		}
	}

	public Map<Long, TableMapEvent> getTableMapEvents() {
		return tableMapEvents;
	}

	public BinlogEventParser getEventParser(int type) {
		return this.parsers[type];
	}
	public BinlogEventParser unregistgerEventParser(int type) {
		return this.parsers[type] = null;
	}
	public void registgerEventParser(BinlogEventParser parser) {
		this.parsers[parser.getEventType()] = parser;
	}

	@Override
	public String getBinlogFileName() {
		return binlogFileName;
	}

	@Override
	public BinlogEventListener getEventListener() {
		return this;
	}

	@Override
	public TableMapEvent getTableMapEvent(long tableId) {
		return this.tableMapEvents.get(tableId);
	}

	@Override
	public void onEvents(BinlogEventV4 event) {
		if (event instanceof TableMapEvent) {
			final TableMapEvent e = (TableMapEvent) event;
			this.tableMapEvents.put(e.getTableId(), e);
		}
		else if (event instanceof RotateEvent) {
			final RotateEvent e = (RotateEvent) event;
			this.binlogFileName = e.getBinlogFileName().toString();
			
			if (isClearTableMapOnRotate()) 
				this.tableMapEvents.clear();
		}
		
		this.parsedEvent = event;
	}

	public boolean isClearTableMapOnRotate() {
		return clearTableMapOnRotate;
	}
	public void setClearTableMapOnRotate(boolean clearTableMapOnRotate) {
		this.clearTableMapOnRotate = clearTableMapOnRotate;
	}
}
