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

import kafka.message.Message;

import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.ObjectReader;
import org.codehaus.jackson.type.TypeReference;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.client.Client;

/**
 * JsonMessageHandler
 * 
 * Handle a simple json message
 * Uses BulkRequestBuilder to send messages in bulk
 * 
 * example format 
 * { "index" : "example_index", 
 *  "type" : "example_type", 
 *  "id" : "asdkljflkasjdfasdfasdf", 
 *  "source" : {"source_data1":"values of source_data1", "source_data2" : 99999 } 
 *  }
 *  
 *  index, type, and source are required
 *  id is optional
 *   
 */
public class JsonMessageHandler extends MessageHandler {

	final ObjectReader reader = new ObjectMapper().reader(new TypeReference<Map<String, Object>>() {});

	private Client client;
	private Map<String, Object> messageMap;

	public JsonMessageHandler(Client client) {
		this.client = client;
	}

	protected void readMessage(Message message) throws Exception {
		messageMap = reader.readValue(getMessageData(message));
	}

	protected String getIndex() {
		return (String) messageMap.get("index");
	}

	protected String getType() {
		return (String) messageMap.get("type");
	}

	protected String getId() {
		return (String) messageMap.get("id");
	}

	protected Map<String, Object> getSource() {
		return (Map<String, Object>) messageMap.get("source");
	}

	protected IndexRequestBuilder createIndexRequestBuilder() {
		// Note: prepareIndex() will automatically create the index if it
		// doesn't exist
		return client.prepareIndex(getIndex(), getType(), getId()).setSource(getSource());
	}

	@Override
	public void handle(BulkRequestBuilder bulkRequestBuilder, Message message) throws Exception {
		this.readMessage(message);
		bulkRequestBuilder.add( this.createIndexRequestBuilder() );
	}

}
