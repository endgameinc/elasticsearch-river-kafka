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

import com.timgroup.statsd.StatsDClient;

public class StatsReporter {

	StatsDClient statsd = null;
	
	String numMsg;
	String flushes;
	String failed;
	String succeeded;
	String rateName;
	String backlog;
	
	public StatsReporter(KafkaRiverConfig riverConfig) {
		if(riverConfig.statsdHost == null){
			return;
		}
		
		statsd = new StatsDClient(riverConfig.statsdPrefix, riverConfig.statsdHost, riverConfig.statsdPort);
		String baseName = String.format("%s.%d.%s.%d", 
				riverConfig.brokerHost, riverConfig.brokerPort,
				riverConfig.topic, riverConfig.partition);
		
		numMsg    = baseName + ".numMsg";
		flushes   = baseName + ".flushes";
		failed    = baseName + ".failed";
		succeeded = baseName + ".succeeded";
		rateName  = baseName + ".rate";
		backlog   = baseName + ".backlog";
	}

	public void reoportStats(Stats stats) {
		if(!isEnabled())
			return;
		
		statsd.count(numMsg, stats.numMessages);
		statsd.count(flushes, stats.flushes);
		statsd.count(failed, stats.failed);
		statsd.count(succeeded, stats.succeeded);
		statsd.gauge(rateName, (int) Math.floor(stats.rate));
		if(stats.backlog > Integer.MAX_VALUE){
			statsd.gauge(backlog, Integer.MAX_VALUE);
		}
		else{
			statsd.gauge(backlog, (int)stats.backlog);
		}
	}
	
	boolean isEnabled() {
		return null != statsd;
	}
}
