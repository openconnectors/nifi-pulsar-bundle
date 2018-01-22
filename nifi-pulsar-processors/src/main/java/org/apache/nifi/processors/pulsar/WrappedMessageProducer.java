/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.nifi.processors.pulsar;

import org.apache.nifi.logging.ComponentLog;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClientException;

public class WrappedMessageProducer {
	
	private final Producer producer;
	private boolean closed = false;
	
	public WrappedMessageProducer(Producer producer) {
		this.producer = producer;
	}

	public void close(ComponentLog logger) {
		this.closed = true;
		
	}

	public Producer getProducer() {
		return this.producer;
	}
	
	public boolean isClosed() {
		return this.closed;
	}

	public boolean send(byte[] messageContent, ComponentLog logger) {
	
		try {
			producer.send(messageContent);
			return true;
		} catch (PulsarClientException e) {
			logger.error("Failed to connect to Pulsar Server due to {}", new Object[]{e});  
			return false;
		}
		
		
	}
}
