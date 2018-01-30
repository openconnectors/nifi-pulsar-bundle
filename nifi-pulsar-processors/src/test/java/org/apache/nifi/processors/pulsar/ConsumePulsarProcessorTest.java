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

import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.TestRunners;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.*;

public class ConsumePulsarProcessorTest extends AbstractPulsarProcessorTest {

    @Mock
	Consumer mockConsumer;
    
    @Mock
	Message mockMessage;
    
    @Rule public MockitoRule mockitoRule = MockitoJUnit.rule();

    @Before
    public void init() throws InitializationException {
        runner = TestRunners.newTestRunner(ConsumePulsar.class);
        
        mockClient = mock(PulsarClient.class);
        mockConsumer = mock(Consumer.class);
        mockMessage = mock(Message.class);
        
        try {
        		when(mockClient.subscribe(anyString(), anyString())).thenReturn(mockConsumer);
			when(mockConsumer.receive()).thenReturn(mockMessage);
		} catch (PulsarClientException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
        
        addPulsarClientService();
    }

    @Test
	public void simpleMessageTest() { 	
    		when(mockMessage.getData()).thenReturn("Mocked Message".getBytes());
    		
    		runner.setProperty(ConsumePulsar.TOPIC, "foo");
    		runner.setProperty(ConsumePulsar.SUBSCRIPTION, "bar");
    		runner.run();
        runner.assertAllFlowFilesTransferred(PublishPulsar.REL_SUCCESS);
    }
    
    @Test
    public void emptyMessageTest() {
    		when(mockMessage.getData()).thenReturn("".getBytes());
		
		runner.setProperty(ConsumePulsar.TOPIC, "foo");
		runner.setProperty(ConsumePulsar.SUBSCRIPTION, "bar");
		runner.run();
		runner.assertAllFlowFilesTransferred(PublishPulsar.REL_SUCCESS);
    }
    
    /*
     * Verify that the consumer gets closed. 
     */
    @Test
    public void onStoppedTest() throws NoSuchMethodException, SecurityException {
    		when(mockMessage.getData()).thenReturn("Mocked Message".getBytes());
		
		runner.setProperty(ConsumePulsar.TOPIC, "foo");
		runner.setProperty(ConsumePulsar.SUBSCRIPTION, "bar");
		runner.run(10, true);
		runner.assertAllFlowFilesTransferred(PublishPulsar.REL_SUCCESS);
		
		runner.assertQueueEmpty();
		
		System.out.println(ConsumePulsar.class.getMethod("close", ProcessContext.class).isBridge()); 
		
    }

}
