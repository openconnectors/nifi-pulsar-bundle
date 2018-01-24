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

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.InputStreamCallback;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.pulsar.PulsarClientService;
import org.apache.nifi.stream.io.StreamUtils;
import org.apache.nifi.util.StringUtils;
import org.apache.pulsar.client.api.PulsarClientException;

@Tags({"Apache", "Pulsar", "Put", "Send", "Message", "PubSub"})
@CapabilityDescription("Sends the contents of a FlowFile as a message to Apache Pulsar using the Pulsar 1.21 Producer API."
    + "The messages to send may be individual FlowFiles or may be delimited, using a "
    + "user-specified delimiter, such as a new-line. "
    + "The complementary NiFi processor for fetching messages is ConsumePulsar.")
@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)

public class PublishPulsar extends AbstractPulsarProcessor {
	
	public static final PropertyDescriptor BATCH_SIZE = new PropertyDescriptor.Builder()
            .name("Message Batch Size")
            .description("The number of messages to pull/push in a single iteration of the processor")
            .required(true)
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .defaultValue("10")
            .build();

	private static final List<PropertyDescriptor> PROPERTIES;
    private static final Set<Relationship> RELATIONSHIPS;
    
    /* This data structure supports one queue per topic, with each queue having infinite length
     * this allows us to reuse producers for a given topic, and kill all the producers if a given
     * topic is experiencing communication issues.
     */
    private final Map<String, Queue<WrappedMessageProducer>> topicProducerQueues = new HashMap<String, Queue<WrappedMessageProducer>> ();
    
    static {
        final List<PropertyDescriptor> properties = new ArrayList<>();
        properties.add(PULSAR_CLIENT_SERVICE);
        properties.add(TOPIC);
        properties.add(BATCH_SIZE);

        PROPERTIES = Collections.unmodifiableList(properties);

        final Set<Relationship> relationships = new HashSet<>();
        relationships.add(REL_SUCCESS);
        relationships.add(REL_FAILURE);
        RELATIONSHIPS = Collections.unmodifiableSet(relationships);
    }
    
    @Override
    public Set<Relationship> getRelationships() {
        return RELATIONSHIPS;
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTIES;
    }
    
    @OnStopped
    public void cleanupResources() {
    	
    		for (Queue<WrappedMessageProducer> producerQueue : topicProducerQueues.values() ) {
    			WrappedMessageProducer wrappedProducer = producerQueue.poll();
    	        while (wrappedProducer != null) {
    	            wrappedProducer.close(getLogger());
    	            wrappedProducer = producerQueue.poll();
    	        }
    		}           
    }
    
	@Override
	public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
		
		final ComponentLog logger = getLogger();
        final List<FlowFile> flowFiles = session.get(context.getProperty(BATCH_SIZE).asInteger().intValue());
        
        if (flowFiles.isEmpty()) {
            return;
        }
        
        final Set<FlowFile> successfulFlowFiles = new HashSet<FlowFile>();
        final Set<FlowFile> failedFlowFiles = new HashSet<FlowFile>();
        final Map<String, Set<WrappedMessageProducer>> producers = new HashMap<String, Set<WrappedMessageProducer>> ();
        
        	// Send each FlowFile to Pulsar asynchronously.
        for (final FlowFile flowFile : flowFiles) {
        	
        		// Read the contents of the FlowFile into a byte array
            final byte[] messageContent = new byte[(int) flowFile.getSize()];
            session.read(flowFile, new InputStreamCallback() {
            		@Override
                public void process(final InputStream in) throws IOException {
            			StreamUtils.fillBuffer(in, messageContent, true);
                }
            });
            
            // Nothing to do, so skip this Flow file.
        		if (messageContent == null || messageContent.length < 1) {
        			continue;
        		}
        	
        		final String topic = context.getProperty(TOPIC).evaluateAttributeExpressions(flowFile).getValue();  
        		
        		if (StringUtils.isBlank(topic)) {
        			logger.error("Invalid topic specified {}", new Object[] {topic});
        			failedFlowFiles.add(flowFile);
        			continue;
        		}
        		
        		WrappedMessageProducer wrappedProducer = getTopicProducerQueue(topic).poll();
        		
        		if (wrappedProducer == null) {
        			try {
        				wrappedProducer = createMessageProducer(context, topic);
        					        	                
        	            if (wrappedProducer.send(messageContent, logger)) {
        	            		successfulFlowFiles.add(flowFile);        	                       	            		
        	            } else {
        	            		failedFlowFiles.add(flowFile);
        	                	// We need to close all of these producers, so they can be flushed
        	            		wrappedProducer.close(logger);
        	            }
        	                
        			} catch (final Exception e) {
        				logger.error("Failed to connect to Pulsar Server due to {}", new Object[]{e});                    
        				/* Keep trying the rest of the Flow files, as each Topic attribute may be different.
        				 * and failure to send to one topic is not indicative of a Pulsar system failure
        				 */
        				failedFlowFiles.add(flowFile);
        			} finally {
        				
        				// Keep track of all the producers used so we can either release them back to the pool if they haven't been closed
        				if ( producers.get(topic) == null ) {
        					producers.put(topic, new HashSet<WrappedMessageProducer> ());
        				}
        				
        				producers.get(topic).add(wrappedProducer);
        			}
        		}
        	}
        
        // Handle successful and failed flow files
        if (!successfulFlowFiles.isEmpty()) {
        		session.transfer(successfulFlowFiles, REL_SUCCESS);
        }
        
        if (!failedFlowFiles.isEmpty()) {
        		session.transfer(failedFlowFiles, REL_FAILURE);
        }
        
        if (producers.isEmpty())
        		return;
        
        // Release the WrappedMessageProducers
        for (String topic : producers.keySet()) {
        		for (WrappedMessageProducer producer : producers.get(topic)) {
        			if (producer != null && !producer.isClosed()) {
        				getTopicProducerQueue(topic).offer(producer);
        			}
        		}
        }
	}
	
	private Queue<WrappedMessageProducer> getTopicProducerQueue(String topic) {
		
		Queue<WrappedMessageProducer> queue = topicProducerQueues.get(topic);
		
		if (queue == null) {
			queue = new LinkedBlockingQueue<>();
			topicProducerQueues.put(topic, queue);
		}
		
		return queue;
	}

	private WrappedMessageProducer createMessageProducer(ProcessContext context, String topic) throws PulsarClientException {
		
		final PulsarClientService pulsarClientService = context.getProperty(PULSAR_CLIENT_SERVICE)
        		.asControllerService(PulsarClientService.class);
		
		return new WrappedMessageProducer(pulsarClientService.getClient().createProducer(topic));
	}

}
