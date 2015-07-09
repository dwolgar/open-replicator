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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.code.or.binlog.BinlogEventV4Header;
import com.google.code.or.binlog.BinlogParserContext;
import com.google.code.or.binlog.UserVariable;
import com.google.code.or.binlog.ext.XChecksum;
import com.google.code.or.binlog.impl.event.UserVarEvent;
import com.google.code.or.binlog.impl.variable.user.UserVariableDecimal;
import com.google.code.or.binlog.impl.variable.user.UserVariableInt;
import com.google.code.or.binlog.impl.variable.user.UserVariableReal;
import com.google.code.or.binlog.impl.variable.user.UserVariableRow;
import com.google.code.or.binlog.impl.variable.user.UserVariableString;
import com.google.code.or.io.XInputStream;

/**
 * 
 * @author Arbore
 */
public class UserVarEventParserExt extends AbstractBinlogEventParserExt {
	//
	private static final Logger LOGGER = LoggerFactory.getLogger(UserVarEventParserExt.class);

	/**
	 * 
	 */
	public UserVarEventParserExt(XChecksum checksum) {
		super(UserVarEvent.EVENT_TYPE, checksum);
	}

	/**
	 * 
	 */
	public void parse(XInputStream is, BinlogEventV4Header header, BinlogParserContext context)
	throws IOException {
		final UserVarEvent event = new UserVarEvent(header);
		event.setVarNameLength(is.readInt(4, checksum));
		event.setVarName(is.readFixedLengthString(event.getVarNameLength(), checksum));
		event.setIsNull(is.readInt(1, checksum));
		if(event.getIsNull() == 0) {
			event.setVarType(is.readInt(1, checksum));
			event.setVarCollation(is.readInt(4, checksum));
			event.setVarValueLength(is.readInt(4, checksum));
			event.setVarValue(parseUserVariable(is, event));
		}
		checksum.validateAndReset(is.readInt(4));//CRC32
		context.getEventListener().onEvents(event);
	}

	/**
	 * 
	 */
	protected UserVariable parseUserVariable(XInputStream is, UserVarEvent event) 
	throws IOException {
		final int type = event.getVarType();
		switch(type) {
		case UserVariableDecimal.TYPE: return new UserVariableDecimal(is.readBytes(event.getVarValueLength(), checksum));
		case UserVariableInt.TYPE: return new UserVariableInt(is.readLong(event.getVarValueLength(), checksum), is.readInt(1, checksum));
		case UserVariableReal.TYPE: return new UserVariableReal(Double.longBitsToDouble(is.readLong(event.getVarValueLength(), checksum)));
		case UserVariableRow.TYPE: return new UserVariableRow(is.readBytes(event.getVarValueLength(), checksum));
		case UserVariableString.TYPE: return new UserVariableString(is.readBytes(event.getVarValueLength(), checksum), event.getVarCollation());
		default: LOGGER.warn("unknown user variable type: " + type); return null;
		}
	}
}
