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

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
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
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.pulsar.PulsarClientService;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.PulsarClientException;

@Tags({"Pulsar", "Get", "Ingest", "Ingress", "Topic", "PubSub", "Consume"})
@CapabilityDescription("Consumes messages from Apache Pulsar "
        + "The complementary NiFi processor for sending messages is PublishPulsar.")
@InputRequirement(InputRequirement.Requirement.INPUT_FORBIDDEN)
public class ConsumePulsar extends AbstractPulsarProcessor {
	
	static final PropertyDescriptor SUBSCRIPTION = new PropertyDescriptor.Builder()
	        .name("Subscription")
	        .displayName("Subscription Name")
	        .description("The name of the Pulsar subscription to consume from.")
	        .required(true)
	        .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
	        .expressionLanguageSupported(true)
	        .build();

	private static final List<PropertyDescriptor> PROPERTIES;
    private static final Set<Relationship> RELATIONSHIPS;
    
    private final Queue<WrappedMessageConsumer> consumerQueue = new LinkedBlockingQueue<>();

    static {
        final List<PropertyDescriptor> properties = new ArrayList<>();
        properties.add(PULSAR_CLIENT_SERVICE);
        properties.add(TOPIC);
        properties.add(SUBSCRIPTION);

        PROPERTIES = Collections.unmodifiableList(properties);

        final Set<Relationship> relationships = new HashSet<>();
        relationships.add(REL_SUCCESS);
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
        WrappedMessageConsumer wrappedConsumer = consumerQueue.poll();
        while (wrappedConsumer != null) {
            wrappedConsumer.close(getLogger());
            wrappedConsumer = consumerQueue.poll();
        }
    }
	
	@Override
	public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
	
		final ComponentLog logger = getLogger();
        WrappedMessageConsumer wrappedConsumer = consumerQueue.poll();
        
        if (wrappedConsumer == null) {
            try {
                wrappedConsumer = createConsumer(context);
            } catch (PulsarClientException e) {
                logger.error("Failed to connect to Pulsar server due to {}", e);
                context.yield();
                return;
            }
        }
        
        try {
            consume(context, session, wrappedConsumer);
        } finally {
            if (!wrappedConsumer.isClosed()) {
                consumerQueue.offer(wrappedConsumer);
            }
        }
        
	}
	
	/*
	 * When this Processor expects to receive many small files, it may 
	 * be advisable to create several FlowFiles from a single session 
	 * before committing the session. Typically, this allows the Framework 
	 * to treat the content of the newly created FlowFiles much more efficiently.
	 */
	private void consume(ProcessContext context, ProcessSession session, WrappedMessageConsumer wrappedConsumer) {
		
		final ComponentLog logger = getLogger();
		final Consumer consumer = wrappedConsumer.getConsumer();
		final Message msg;
		FlowFile flowFile = null;
		
        try {
						
        		msg = consumer.receive();
        		final byte[] value = msg.getData();

        		if (value != null && value.length > 0) {
        			flowFile = session.create();
        			flowFile = session.write(flowFile, out -> {
        				out.write(value);
        			});

        			// session.getProvenanceReporter().receive(flowFile, context.getProperty(URL).getValue());
                session.transfer(flowFile, REL_SUCCESS);
                logger.info("Created {} from {} messages received from Pulsar Server and transferred to 'success'",
                            new Object[]{flowFile, 1});
                
                session.commit();
                
                /*
                 * This Processor acknowledges receipt of the data and/or removes the data 
                 * from the external source in order to prevent receipt of duplicate files. 
                 * This is done only after the ProcessSession by which the FlowFile was created 
                 * has been committed! Failure to adhere to this principle may result in data 
                 * loss, as restarting NiFi before the session has been committed will result 
                 * in the temporary file being deleted. Note, however, that it is possible using 
                 * this approach to receive duplicate data because the application could be 
                 * restarted after committing the session and before acknowledging or removing 
                 * the data from the external source. In general, though, potential data duplication 
                 * is preferred over potential data loss.
                 */
        			consumer.acknowledge(msg);
        		}
					
		} catch (PulsarClientException e) {		
			logger.error("Failed to receive Pulsar Message due to {}", e);
			wrappedConsumer.close(logger);
			
			if (flowFile != null)
				session.remove(flowFile);
		}

	}
	
	private WrappedMessageConsumer createConsumer(ProcessContext context) throws PulsarClientException {
		final PulsarClientService pulsarClientService = context.getProperty(PULSAR_CLIENT_SERVICE)
        		.asControllerService(PulsarClientService.class);
		
        return new WrappedMessageConsumer(pulsarClientService.getClient().subscribe(context.getProperty(TOPIC).getValue(), 
				context.getProperty(SUBSCRIPTION).getValue()));
	}

}
