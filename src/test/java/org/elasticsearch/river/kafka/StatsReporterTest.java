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

import junit.framework.TestCase;

import org.elasticsearch.common.collect.ImmutableMap;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.river.RiverSettings;

public class StatsReporterTest extends TestCase {

	public void reoportStats() throws Exception {
		Settings globalSettings = ImmutableSettings.settingsBuilder().put("cluster.name", "jason-hfs-cluster").build();
		Map<String, Object> config = new HashMap<>();
		config.put("statsd", ImmutableMap.<String, Object> builder().put("host", "localhost").put("port", "1234").put("prefix", "my_prefix").build());

		StatsReporter r = new StatsReporter(new KafkaRiverConfig(new RiverSettings(globalSettings, config)));
		Stats s = new Stats();
		
		// cant mock statsdclient - due to it being final (Easymock creates a
		// subclass to work its magic
		//
		// ALSO count and gauge calls to StatsDClient are no blocking and will
		// not throw exception, so basically not much to test here
		//

		try {
			r.reoportStats(s);
		} catch (Exception e) {
			fail("This should not fail : " + e.toString());
		}

		s = null;
		
		boolean thrown = false;
		try {
			r.reoportStats(s);
			fail("Never should have gotten here: ");
		} catch (NullPointerException e) {
			thrown = true;
		}
		
		assertTrue(thrown);
	}

	public void testNormal() {
		Settings globalSettings = ImmutableSettings.settingsBuilder().put("cluster.name", "jason-hfs-cluster").build();
		Map<String, Object> config = new HashMap<>();
		config.put("statsd", ImmutableMap.<String, Object> builder().put("host", "localhost").put("port", "1234").put("prefix", "my_prefix").build());

		StatsReporter r = new StatsReporter(new KafkaRiverConfig(new RiverSettings(globalSettings, config)));
		assertTrue(r.isEnabled());

	}

	public void testNotConfigured() {
		Settings globalSettings = ImmutableSettings.settingsBuilder().put("cluster.name", "jason-hfs-cluster").build();
		Map<String, Object> config = new HashMap<>();
		// no statsd config at all
		assertFalse(new StatsReporter(new KafkaRiverConfig(new RiverSettings(globalSettings, config))).isEnabled());

		// missing host
		config.put("statsd", ImmutableMap.<String, Object> builder().put("port", "1234").put("prefix", "my_prefix").build());
		assertFalse(new StatsReporter(new KafkaRiverConfig(new RiverSettings(globalSettings, config))).isEnabled());
	}
}
