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

import kafka.api.FetchRequest;
import kafka.api.OffsetRequest;
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.javaapi.message.ByteBufferMessageSet;

import org.apache.zookeeper.CreateMode;

import com.netflix.curator.framework.CuratorFramework;
import com.netflix.curator.framework.CuratorFrameworkFactory;
import com.netflix.curator.retry.RetryNTimes;

public class KafkaClient {
	
	CuratorFramework curator;
	SimpleConsumer consumer;
	String brokerURL;
	
	public KafkaClient(String zk, String broker, int port)
	{
		brokerURL = broker+":"+port;
		connect(zk, broker, port);
	}
	
	void connect(String zk, String broker, int port)
	{
		try {
			consumer = new SimpleConsumer(broker, port, 1000, 1024*1024*10);
			curator = CuratorFrameworkFactory.newClient(zk, 1000, 15000, new RetryNTimes(5, 2000));
			curator.start();
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}
	
	public void save(String path, String data)
	{
		try {
			if(curator.checkExists().forPath(path) == null){
				curator.create().creatingParentsIfNeeded().withMode(CreateMode.PERSISTENT).forPath(path, data.getBytes());
			}
			else{
				curator.setData().forPath(path, data.getBytes());
			}
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}
	
	public String get(String path) {
		try {
			if (curator.checkExists().forPath(path) != null) {
				return new String(curator.getData().forPath(path));
			} else {
				return null;
			}
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}
	
	public void saveOffset(String topic, int partition, long offset)
	{
		save(String.format("/es-river-kafka/offsets/%s/%s/%d", brokerURL, topic, partition), Long.toString(offset));				
	}
	
	public long getOffset(String topic, int partition) {
		String data = get(String.format("/es-river-kafka/offsets/%s/%s/%d", brokerURL, topic, partition));
		if(data == null)
			return 0;
		return Long.parseLong(data);
	}
	
	public long getNewestOffset(String topic, int partition) {
		return consumer.getOffsetsBefore(topic, partition, OffsetRequest.LatestTime(), 1)[0];
	}
	
	public long getOldestOffset(String topic, int partition) {
		return consumer.getOffsetsBefore(topic, partition, OffsetRequest.EarliestTime(), 1)[0];
	}
	
	ByteBufferMessageSet fetch(String topic, int partition, long offset, int maxSizeBytes)
	{
		return consumer.fetch(new FetchRequest(topic, partition, offset, maxSizeBytes));
	}
	
	public void close() {
		curator.close();
	}
}
