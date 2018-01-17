package org.apache.nifi.processors.pulsar;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Set;

import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.DataUnit;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.InputStreamCallback;
import org.apache.nifi.processor.util.FlowFileFilters;
import org.apache.nifi.pulsar.PulsarClientService;
import org.apache.pulsar.client.api.Producer;

@Tags({"Apache", "Pulsar", "Put", "Send", "Message", "PubSub"})
@CapabilityDescription("Sends the contents of a FlowFile as a message to Apache Pulsar using the Pulsar 1.21 Producer API."
    + "The messages to send may be individual FlowFiles or may be delimited, using a "
    + "user-specified delimiter, such as a new-line. "
    + "The complementary NiFi processor for fetching messages is ConsumePulsar.")
@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)

public class PublishPulsar extends AbstractPulsarProcessor {

    @Override
    public Set<Relationship> getRelationships() {
        return super.getRelationships();
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return super.getPropertyDescriptors();
    }

    
	@Override
	public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
		
		final List<FlowFile> flowFiles = session.get(FlowFileFilters.newSizeBasedFilter(250, DataUnit.KB, 500));
        if (flowFiles.isEmpty()) {
            return;
        }
        
        final PulsarClientService pulsarClientService = context.getProperty(PULSAR_CLIENT_SERVICE)
        		.asControllerService(PulsarClientService.class);
		
        if (pulsarClientService == null) {
            context.yield();
            return;
        }
        
        	// Send each FlowFile to Pulsar asynchronously.
        for (final FlowFile flowFile : flowFiles) {
            	
        		// The topic could change for EACH flowfile
        		final String topic = context.getProperty(TOPIC).evaluateAttributeExpressions(flowFile).getValue();
                
        		session.read(flowFile, new InputStreamCallback() {
        			@Override
        			public void process(final InputStream rawIn) throws IOException {
        				try (final InputStream in = new BufferedInputStream(rawIn)) {
        					Producer producer = pulsarClientService.getClient().createProducer(topic);
        					producer.sendAsync(rawIn.readAllBytes());
        					rawIn.close();
        					producer.close();
        				}
        			}
        		});
        }
        		   
	}

}
