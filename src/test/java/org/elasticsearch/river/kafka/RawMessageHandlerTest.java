/* Copyright 2013 Endgame, Inc.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.river.kafka;

import static org.easymock.EasyMock.aryEq;
import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.eq;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import junit.framework.TestCase;
import kafka.message.Message;

import org.elasticsearch.action.bulk.BulkRequestBuilder;

public class RawMessageHandlerTest extends TestCase {
	public void testIt() throws Exception
	{
		byte[] data = "somedata".getBytes();
		List<String> l = new ArrayList<>();
		
		MessageHandler m = new RawMessageHandler();
		Message message = createMock(Message.class);
		expect(message.payload()).andReturn(ByteBuffer.wrap(data));
		
		BulkRequestBuilder bulkRequestBuilder = createMock(BulkRequestBuilder.class);
		expect(bulkRequestBuilder.add(aryEq(data), eq(0), eq(data.length), eq(false))).andReturn(null);
		replay(message, bulkRequestBuilder);
		
		m.handle(bulkRequestBuilder, message);
		verify(bulkRequestBuilder, message);
	}
}
