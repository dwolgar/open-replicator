package com.google.code.or.net.impl.packet.command;

import java.io.IOException;

import com.google.code.or.common.util.MySQLConstants;
import com.google.code.or.io.util.XSerializer;

public class ComPing extends AbstractCommandPacket {
	private static final long serialVersionUID = -5937563399532599916L;

	public ComPing() {
		super(MySQLConstants.COM_PING);
	}

	@Override
	public byte[] getPacketBody() throws IOException {
		final XSerializer ps = new XSerializer();
	    ps.writeInt(this.command, 1);
		return ps.toByteArray();
	}

}
