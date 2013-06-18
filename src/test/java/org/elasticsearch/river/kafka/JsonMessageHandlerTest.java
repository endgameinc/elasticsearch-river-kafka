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

import static org.easymock.EasyMock.anyObject;
import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

import junit.framework.TestCase;
import kafka.message.Message;

import org.codehaus.jackson.map.ObjectMapper;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.client.Client;

public class JsonMessageHandlerTest extends TestCase {
	
	private String toJson(Object value) {
		ObjectMapper mapper = new ObjectMapper();
		try {
			return mapper.writeValueAsString(value);
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}
	
	Map<String, Object> rec = new HashMap<String, Object>(){{
		put("index", "xyz");
		put("type", "datatype1");
		put("source", new HashMap<String, Object>(){{put("field", "1");}});
	}};
	
	public void testReadMessage() throws Exception
	{
		JsonMessageHandler h = new JsonMessageHandler(null);	
		byte[] json = toJson(rec).getBytes();
		Message message = createMock(Message.class);
		expect(message.payload()).andReturn(ByteBuffer.wrap(json));
		replay(message);
		
		try {
			h.readMessage(message);
		} catch (Exception e) {
			fail("This should not fail");
		}
		
		verify(message);
	}
	
	public void testGettersFromReadMessageReturnedMap() throws Exception
	{
		JsonMessageHandler h = new JsonMessageHandler(null);	
		byte[] json = toJson(rec).getBytes();
		Message message = createMock(Message.class);
		
		expect(message.payload()).andReturn(ByteBuffer.wrap(json));
		replay(message);
		
		try {
			h.readMessage(message);
		} catch (Exception e) {
			fail("This should not fail");
		}
		
		assertEquals(h.getIndex(), rec.get("index"));
		assertEquals(h.getType(), rec.get("type"));
		assertEquals(h.getSource(), rec.get("source"));
		assertEquals(h.getId(), rec.get("id"));
		verify(message);
	}
	
	public void testIt() throws Exception
	{
		Client client = createMock(Client.class);
		IndexRequestBuilder irb = createMock(IndexRequestBuilder.class);
		JsonMessageHandler h = new JsonMessageHandler(client);
		byte[] json = toJson(rec).getBytes();
				
		expect(client.prepareIndex(anyObject(String.class), anyObject(String.class), anyObject(String.class))).andReturn(irb);
		replay(client);
		
		Message message = createMock(Message.class);
		expect(message.payload()).andReturn(ByteBuffer.wrap(json));
		replay(message);
		
		BulkRequestBuilder bulkRequestBuilder = createMock(BulkRequestBuilder.class);
		
		expect(bulkRequestBuilder.add(anyObject(IndexRequestBuilder.class))).andReturn(null);
		replay(bulkRequestBuilder);
		
		try {
			h.handle(bulkRequestBuilder, message);
		} catch (Exception e) {
			fail("This should not fail");
		}
		
		verify(client);
	}
}
