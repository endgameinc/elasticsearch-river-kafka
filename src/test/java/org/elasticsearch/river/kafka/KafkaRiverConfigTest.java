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

import java.util.HashMap;
import java.util.Map;

import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.river.RiverSettings;

import junit.framework.TestCase;

public class KafkaRiverConfigTest extends TestCase {
	public void testIt()
	{
		Map<String, Object> kafka = new HashMap<>();
		kafka.put("zookeeper", "zoo-host");
		kafka.put("broker_host", "broker-host");
		kafka.put("broker_port", "9999");
		kafka.put("topic", "my_topic");
		kafka.put("partition", "777");
		kafka.put("message_handler_factory_class", "my.factory.class.MyFactory");
		
		Map<String, Object> index = new HashMap<>();
		index.put("bulk_size_bytes", "1717171");
		index.put("bulk_timeout", "111ms");
		
		Map<String, Object> statsd = new HashMap<>();
		statsd.put("host", "some.host");
		statsd.put("port", "1234");
		statsd.put("prefix", "boo.yeah");
		
		Map<String, Object> map = new HashMap<>();
		map.put("kafka", kafka);
		map.put("index", index);
		map.put("statsd", statsd);
		
		RiverSettings settings = new RiverSettings(
			ImmutableSettings.settingsBuilder().build(), 
			map);
		KafkaRiverConfig c = new KafkaRiverConfig(settings);
		
		assertEquals("broker-host", c.brokerHost);
		assertEquals(9999, c.brokerPort);
		assertEquals("zoo-host", c.zookeeper);
		assertEquals("my.factory.class.MyFactory", c.factoryClass);
		assertEquals(1717171, c.bulkSize);
		assertEquals(777, c.partition);
		assertEquals("my_topic", c.topic);
		assertEquals(111, c.bulkTimeout.millis());
		
		
		assertEquals(1234, c.statsdPort);
		assertEquals("some.host", c.statsdHost);
		assertEquals("boo.yeah", c.statsdPrefix);
	}
	
	public void testDefaults()
	{
		Map<String, Object> map = new HashMap<>();		
		RiverSettings settings = new RiverSettings(
			ImmutableSettings.settingsBuilder().build(), 
			map);
		KafkaRiverConfig c = new KafkaRiverConfig(settings);
		
		assertEquals("localhost", c.brokerHost);
		assertEquals(9092, c.brokerPort);
		assertEquals("localhost", c.zookeeper);
		assertEquals(10485760, c.bulkSize);
		assertEquals(0, c.partition);
		assertEquals("default_topic", c.topic);
		assertEquals(10000, c.bulkTimeout.millis());
		
		assertEquals("org.elasticsearch.river.kafka.JsonMessageHandlerFactory", c.factoryClass);
		
		assertEquals(-1, c.statsdPort);
		assertNull(c.statsdHost);
		assertNull(c.statsdPrefix);
		
	}
}
