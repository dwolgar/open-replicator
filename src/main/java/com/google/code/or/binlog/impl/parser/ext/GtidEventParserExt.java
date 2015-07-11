package com.google.code.or.binlog.impl.parser.ext;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import com.google.code.or.binlog.BinlogEventV4Header;
import com.google.code.or.binlog.BinlogParserContext;
import com.google.code.or.binlog.ext.XChecksum;
import com.google.code.or.binlog.impl.event.GtidEvent;
import com.google.code.or.common.util.MySQLConstants;
import com.google.code.or.io.XInputStream;

/**
 * GTID Event
 * 
 * <p>
 * Event format:
 * 
 * <pre>
 *         +-------------------+
 *         | 1B commit flag    |
 *         +-------------------+
 *         | 16B Source ID     |
 *         +-------------------+
 *         | 8B Txn ID         |
 *         +-------------------+
 *         | ...               |
 *         +-------------------+
 *     </pre>
 * </p>
 */
public class GtidEventParserExt extends AbstractBinlogEventParserExt {

  public GtidEventParserExt(XChecksum checksum) {
    super(MySQLConstants.GTID_LOG_EVENT, checksum);
  }

  @Override
  public void parse(XInputStream is, BinlogEventV4Header header, BinlogParserContext context)
      throws IOException {
    is.readBytes(1, checksum); // commit flag, always true
    byte[] sourceId = is.readBytes(16, checksum);
    long transactionId =
        ByteBuffer.wrap(is.readBytes(8, checksum)).order(ByteOrder.LITTLE_ENDIAN).getLong();
    is.skip(is.available()); // position at next event
    checksum.reset();// CRC32
    GtidEvent event = new GtidEvent(sourceId, transactionId);
    event.setHeader(header);

    context.getEventListener().onEvents(event);
  }
}
