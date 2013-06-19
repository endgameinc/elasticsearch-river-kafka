Kafka River Plugin for ElasticSearch
==================================

The Kafka River plugin allows index bulk format messages into elasticsearch.

1. Download & Build Kafka
	
	See [Apacke Kafka Quick Start Guide](http://kafka.apache.org/07/quickstart.html)  for instructions on how to Download and Build.

	If you are installing on an encyrpted Ubuntu drive you may get "File name too long" error during the build. 
	This can be solved by building on an unencrypted file system and moving the files to your desired install point. 

2. install kafka in your maven repo:

        mvn install:install-file -Dfile=./core/target/scala_2.8.0/kafka-0.7.2.jar -DgroupId=org.apache.kafka \
            -DartifactId=kafka -Dversion=0.7.2 -Dpackaging=jar

3. Build this plugin:

        mvn compile test package 
        # this will create a file here: target/releases/elasticsearch-river-kafka-1.0.1-SNAPSHOT.zip
        PLUGIN_PATH=`pwd`/target/releases/elasticsearch-river-kafka-1.0.1-SNAPSHOT.zip

4. Install the PLUGIN

        cd $ELASTICSEARCH_HOME
        ./bin/plugin -url file:/$PLUGIN_PATH -install elasticsearch-river-kafka

5. Updating the plugin

        cd $ELASTICSEARCH_HOME
        ./bin/plugin -remove elasticsearch-river-kafka
        ./bin/plugin -url file:/$PLUGIN_PATH -install elasticsearch-river-kafka

##### Version Support

ElasticSearch version 0.90.0


Deployment
==========

Creating the Kafka river is as simple as (all configuration parameters are provided, with default values):

    bulk_size_bytes - max size of messages to pull from Kafka each request
    bulk_timeout - socket timeout for Kafka

	curl -XPUT 'localhost:9200/_river/my_kafka_river_0/_meta' -d '{
	    "type" : "kafka",
	    "kafka" : {
	        "broker_host" : "localhost", 
			"message_handler_factory_class" : "org.elasticsearch.river.kafka.JsonMessageHandlerFactory",
	        "zookeeper" : "localhost",
	        "topic" : "my_topic",
	        "partition" : "0",
	        "broker_port" : 9092
	    },
	    "index" : {
	        "bulk_size_bytes" : 10000000,
	        "bulk_timeout" : "1000ms"
        },
        "statsd":{
            "prefix": "es-kafka-river",
            "host": "ambassador",
            "port": "8125"
        }

	}'

Kafka offsets are stored in zookeeper.

NOTE: in its current form, this River only reads from a single broker and a single partition.  This will likely change in the future.  In 
order to consume from multiple partitions and multiple brokers, multiple rivers need to be configured.

	curl -XPUT 'localhost:9200/_river/my_kafka_river_0/_meta' -d '{
	    "type" : "kafka",
	    "kafka" : {
	        "broker_host" : "localhost", 
			"message_handler_factory_class" : "org.elasticsearch.river.kafka.JsonMessageHandlerFactory",
	        "zookeeper" : "localhost",
	        "topic" : "my_topic",
	        "partition" : "0",
	        "broker_port" : 9092
	    },
	    "index" : {
	        "bulk_size_bytes" : 10000000,
	        "bulk_timeout" : "1000ms"
        },
        "statsd":{
            "prefix": "es-kafka-river",
            "host": "ambassador",
            "port": "8125"
        }

	}'
	curl -XPUT 'localhost:9200/_river/my_kafka_river_1/_meta' -d '{
	    "type" : "kafka",
	    "kafka" : {
	        "broker_host" : "localhost", 
			"message_handler_factory_class" : "org.elasticsearch.river.kafka.JsonMessageHandlerFactory",
	        "zookeeper" : "localhost",
	        "topic" : "my_topic",
	        "partition" : "1",
	        "broker_port" : 9092
	    },
	    "index" : {
	        "bulk_size_bytes" : 10000000,
	        "bulk_timeout" : "1000ms"
        },
        "statsd":{
            "prefix": "es-kafka-river",
            "host": "ambassador",
            "port": "8125"
        }

	}'
	curl -XPUT 'localhost:9200/_river/my_kafka_river_2/_meta' -d '{
	    "type" : "kafka",
	    "kafka" : {
	        "broker_host" : "localhost", 
			"message_handler_factory_class" : "org.elasticsearch.river.kafka.JsonMessageHandlerFactory",
	        "zookeeper" : "localhost",
	        "topic" : "my_topic",
	        "partition" : "2",
	        "broker_port" : 9092
	    },
	    "index" : {
	        "bulk_size_bytes" : 10000000,
	        "bulk_timeout" : "1000ms"
        },
        "statsd":{
            "prefix": "es-kafka-river",
            "host": "ambassador",
            "port": "8125"
        }

	}'
	curl -XPUT 'localhost:9200/_river/my_kafka_river_3/_meta' -d '{
	    "type" : "kafka",
	    "kafka" : {
	        "broker_host" : "localhost", 
			"message_handler_factory_class" : "org.elasticsearch.river.kafka.JsonMessageHandlerFactory",
	        "zookeeper" : "localhost",
	        "topic" : "my_topic",
	        "partition" : "3",
	        "broker_port" : 9092
	    },
	    "index" : {
	        "bulk_size_bytes" : 10000000,
	        "bulk_timeout" : "1000ms"
        },
        "statsd":{
            "prefix": "es-kafka-river",
            "host": "ambassador",
            "port": "8125"
        }

	}'
    
The river is automatically bulking queue messages if the queue is overloaded, allowing for faster catchup with the 
messages streamed into the queue. The `ordered` flag allows to make sure that the messages will be indexed in the 
same order as they arrive in the query by blocking on the bulk request before picking up the next data to be indexed. 
It can also be used as a simple way to throttle indexing.

If `message_handler_factory_class` is not set it will use the `JsonMessageHandlerFactory` and will expect json messages from Kafka with this format:

	{	
		"index" : "example_index", 
		"type" : "example_type", 
		"id" : "asdkljflkasjdfasdfasdf", 
		"source" : { ..... } 
	}   

License
-------

elasticsearch-river-kafka
	
Copyright 2013 [Endgame, Inc.](http://www.endgame.com/)

![Endgame, Inc.](http://www.endgame.com/images/navlogo.png)
	
This product includes software plugin developed for
ElasticSearch and Shay Banon – [Elasticsearch](http://www.elasticsearch.org/)
	
Inspiration was taken from David Pilato and his ElasticSearch Rabbit MQ Plugin  
https://github.com/elasticsearch/elasticsearch-river-rabbitmq

	Licensed under the Apache License, Version 2.0 (the "License"); you may
	not use this file except in compliance with the License. You may obtain
	a copy of the License at

	     http://www.apache.org/licenses/LICENSE-2.0

	Unless required by applicable law or agreed to in writing,
	software distributed under the License is distributed on an
	"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
	KIND, either express or implied.  See the License for the
	specific language governing permissions and limitations
	under the License.

Contributors
-------------

 - [Jason Trost](https://github.com/jt6211/)
 - [Mark Conlin](https://github.com/meconlin)


