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

import junit.framework.TestCase;

public class StatsTest extends TestCase {

	public void testIt()
	{
		Stats s = new Stats();
		
		assertEquals(0, s.failed);
		assertEquals(0, s.succeeded);
		assertEquals(0, s.flushes);
		assertEquals(0, s.numMessages);
		assertEquals(0.0, s.rate);
		
		s.failed = 50;
		s.succeeded = 50;
		s.flushes = 50;
		s.numMessages = 50;
		s.rate = 50.0;
		
		s.reset();
		
		assertEquals(0, s.failed);
		assertEquals(0, s.succeeded);
		assertEquals(0, s.flushes);
		assertEquals(0, s.numMessages);
		assertEquals(0.0, s.rate);
	}
}
