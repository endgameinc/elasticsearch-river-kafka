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
import static org.easymock.EasyMock.aryEq;
import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.eq;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.expectLastCall;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;

import java.util.Collections;

import junit.framework.TestCase;
import kafka.api.FetchRequest;
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.javaapi.message.ByteBufferMessageSet;

import org.apache.zookeeper.data.Stat;

import com.netflix.curator.framework.CuratorFramework;
import com.netflix.curator.framework.api.ExistsBuilder;
import com.netflix.curator.framework.api.GetDataBuilder;
import com.netflix.curator.framework.api.SetDataBuilder;

public class KafkaClientTest extends TestCase {
	
	public ExistsBuilder mockExistsBuilder(String path, boolean exists) throws Exception
	{
		ExistsBuilder b = createMock(ExistsBuilder.class);
		if(exists)
		{
			expect(b.forPath(eq(path))).andReturn(new Stat());
		}
		else
		{
			expect(b.forPath(eq(path))).andReturn(null);
		}
		replay(b);		
		return b;
	}
	
	public GetDataBuilder mockGetDataBuilder(String path, byte[] data) throws Exception
	{
		GetDataBuilder b = createMock(GetDataBuilder.class);
		expect(b.forPath(eq(path))).andReturn(data);
		replay(b);
		return b;
	}
	
	public SetDataBuilder mockSetDataBuilder(String path, byte[] data) throws Exception
	{
		SetDataBuilder b = createMock(SetDataBuilder.class);
		expect(b.forPath(eq(path), aryEq(data))).andReturn(new Stat());
		replay(b);
		return b;
	}
	
	CuratorFramework mockCurator;
	SimpleConsumer mockConsumer;
	KafkaClient client;
	
	@Override
	protected void setUp() throws Exception {
		super.setUp();
		mockCurator = createMock(CuratorFramework.class);
		mockConsumer = createMock(SimpleConsumer.class);
		
		final CuratorFramework cur = mockCurator;
		final SimpleConsumer con = mockConsumer;
		client = new KafkaClient("zookeeper", "broker", 9092){
			void connect(String zk, String broker, int port) 
			{
				this.curator = cur;
				this.consumer = con;
			};
		};
	}
	
	@Override
	protected void tearDown() throws Exception {
		super.tearDown();
		mockCurator = null;
		mockConsumer = null;
		client = null;
	}

	public void testConstructor()
	{
		replay(mockConsumer, mockCurator);
		assertEquals(client.brokerURL, "broker:9092");
	}
	
	public void testGetNewestOffset() throws Exception
	{
		long[] mylong = new long[3];
		mylong[0] = 10;
		mylong[1] = 20;
		mylong[2] = 30;
		
		expect(mockConsumer.getOffsetsBefore("topic", 1, -1, 1)).andReturn(mylong);	
		replay(mockConsumer, mockCurator);		
		long answer = client.getNewestOffset("topic", 1);
		assertEquals(mylong[0], answer);
		verify(mockConsumer, mockCurator);
	}

	public void testGetOldestOffset() throws Exception
	{
		long[] mylong = new long[3];
		mylong[0] = 10;
		mylong[1] = 20;
		mylong[2] = 30;
		
		expect(mockConsumer.getOffsetsBefore("topic", 1, -2, 1)).andReturn(mylong);	
		replay(mockConsumer, mockCurator);		
		long answer = client.getOldestOffset("topic", 1);
		assertEquals(mylong[0], answer);
		verify(mockConsumer, mockCurator);
	}
	
	public void testGet() throws Exception
	{
		expect(mockCurator.checkExists()).andReturn(mockExistsBuilder("/some/path", false));
		expect(mockCurator.checkExists()).andReturn(mockExistsBuilder("/some/existing/path", true));
		expect(mockCurator.getData()).andReturn(mockGetDataBuilder("/some/existing/path", "this is data".getBytes()));
		
		replay(mockConsumer, mockCurator);
		assertNull(client.get("/some/path"));
		assertEquals(client.get("/some/existing/path"), "this is data");
		verify(mockConsumer, mockCurator);
	}
	
	public void testSave() throws Exception
	{
		expect(mockCurator.checkExists()).andReturn(mockExistsBuilder("/some/existing/path", true));
		expect(mockCurator.setData()).andReturn(mockSetDataBuilder("/some/existing/path", "this is data".getBytes()));
		
		replay(mockConsumer, mockCurator);
		client.save("/some/existing/path", "this is data");
		verify(mockConsumer, mockCurator);
	}
	
	static class Args
	{
		String path = "";
		String data = "";
	}
	
	public void testSaveOffset()
	{
		final Args args = new Args();
		replay(mockConsumer, mockCurator);
		client = new KafkaClient("zookeeper", "broker", 9092){
			void connect(String zk, String broker, int port) 
			{
				this.curator = mockCurator;
				this.consumer = mockConsumer;
			}
			
			@Override
			public void save(String path, String data) {
				args.path = path;
				args.data = data;
			}
		};
		client.saveOffset("my_topic", 77, 4242);

		assertEquals("/es-river-kafka/offsets/broker:9092/my_topic/77", args.path);
		assertEquals("4242", args.data);
	}
	
	public void testGetOffsets()
	{
		final Args args = new Args();
		replay(mockConsumer, mockCurator);
		client = new KafkaClient("zookeeper", "broker", 9092){
			void connect(String zk, String broker, int port) 
			{
				this.curator = mockCurator;
				this.consumer = mockConsumer;
			}
		
			@Override
			public String get(String path) {
				args.path = path;
				return "100";
			}
		};
		
		assertEquals(100, client.getOffset("my_topic", 777));
		assertEquals("/es-river-kafka/offsets/broker:9092/my_topic/777", args.path);
	}
	
	public void testClose()
	{
		mockCurator.close();
		expectLastCall().asStub();
		replay(mockConsumer, mockCurator);
		
		client.close();
		verify(mockConsumer, mockCurator);
	}
	
	public void testFetch()
	{
		expect(mockConsumer.fetch(anyObject(FetchRequest.class))).andReturn(new ByteBufferMessageSet(Collections.EMPTY_LIST));	
		replay(mockConsumer, mockCurator);		
		client.fetch("my_topic", 0, 1717, 1024);
		verify(mockConsumer, mockCurator);
	}
	
}
