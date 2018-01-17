package org.apache.nifi.processors.pulsar;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.pulsar.PulsarClientService;

public abstract class AbstractPulsarProcessor extends AbstractProcessor {

	static final PropertyDescriptor TOPIC = new PropertyDescriptor.Builder()
	        .name("topic")
	        .displayName("Topic Name")
	        .description("The name of the Pulsar Topic to publish to.")
	        .required(true)
	        .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
	        .expressionLanguageSupported(true)
	        .build();
	
	public static final PropertyDescriptor PULSAR_CLIENT_SERVICE = new PropertyDescriptor.Builder()
			  .name("Pulsar Client Service")
			  .description("Specified the Pulsar Client Service that can be used to create Pulsar connections")
			  .required(true)
			  .identifiesControllerService(PulsarClientService.class)
			  .build();

	static final Relationship REL_SUCCESS = new Relationship.Builder()
	        .name("success")
	        .description("FlowFiles for which all content was sent to Pulsar.")
	        .build();

	static final Relationship REL_FAILURE = new Relationship.Builder()
	        .name("failure")
	        .description("Any FlowFile that cannot be sent to Pulsar will be routed to this Relationship")
	        .build();
	
	private static final List<PropertyDescriptor> PROPERTIES;
    private static final Set<Relationship> RELATIONSHIPS;

    static {
        final List<PropertyDescriptor> properties = new ArrayList<>();
        properties.add(TOPIC);

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
    
}
