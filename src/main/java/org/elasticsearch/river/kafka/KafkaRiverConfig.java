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

import java.util.Map;

import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.river.RiverSettings;

public class KafkaRiverConfig {
	
	public final String zookeeper;
	public final String factoryClass;	// full class path and name for the concrete message handler class factory
	public final String brokerHost;
	public final int brokerPort;
	public final String topic;
	public final int partition;
	
	public final String statsdPrefix;
	public final String statsdHost;
	public final int statsdPort;
	
	public final int bulkSize;
	public final TimeValue bulkTimeout;
    
    public KafkaRiverConfig(RiverSettings settings)
    {
    	if (settings.settings().containsKey("kafka")) {
            Map<String, Object> kafkaSettings = (Map<String, Object>) settings.settings().get("kafka");
            
            topic = (String)kafkaSettings.get("topic");
            zookeeper = XContentMapValues.nodeStringValue(kafkaSettings.get("zookeeper"), "localhost");
            factoryClass = XContentMapValues.nodeStringValue(kafkaSettings.get("message_handler_factory_class"), "org.elasticsearch.river.kafka.JsonMessageHandlerFactory");
            brokerHost = XContentMapValues.nodeStringValue(kafkaSettings.get("broker_host"), "localhost");
            brokerPort = XContentMapValues.nodeIntegerValue(kafkaSettings.get("broker_port"), 9092);
            partition = XContentMapValues.nodeIntegerValue(kafkaSettings.get("partition"), 0);
        }
    	else
    	{
    		zookeeper = "localhost";
    		brokerHost = "localhost";
    		brokerPort = 9092;
    		topic = "default_topic";
    		partition = 0;
    		factoryClass = "org.elasticsearch.river.kafka.JsonMessageHandlerFactory";
    	}
        
        if (settings.settings().containsKey("index")) {
            Map<String, Object> indexSettings = (Map<String, Object>) settings.settings().get("index");
            bulkSize = XContentMapValues.nodeIntegerValue(indexSettings.get("bulk_size_bytes"), 10*1024*1024);
            if (indexSettings.containsKey("bulk_timeout")) {
                bulkTimeout = TimeValue.parseTimeValue(XContentMapValues.nodeStringValue(indexSettings.get("bulk_timeout"), "10ms"), TimeValue.timeValueMillis(10000));
            } else {
                bulkTimeout = TimeValue.timeValueMillis(10);
            }
        } else {
            bulkSize = 10*1024*1024;
            bulkTimeout = TimeValue.timeValueMillis(10000);
        }
        
        if (settings.settings().containsKey("statsd")) {
            Map<String, Object> statsdSettings = (Map<String, Object>) settings.settings().get("statsd");
            statsdHost = (String)statsdSettings.get("host");
            statsdPort = XContentMapValues.nodeIntegerValue(statsdSettings.get("port"), 8125);
            statsdPrefix = XContentMapValues.nodeStringValue(statsdSettings.get("prefix"), "es-kafka-river");
        }
    	else
    	{
    		statsdHost = null;
    		statsdPort = -1;
    		statsdPrefix = null;
    	}
    }
}
