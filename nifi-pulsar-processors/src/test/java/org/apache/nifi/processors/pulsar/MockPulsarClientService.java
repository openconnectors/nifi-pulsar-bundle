package org.apache.nifi.processors.pulsar;

import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.pulsar.PulsarClientPool;
import org.apache.nifi.pulsar.PulsarConsumer;
import org.apache.nifi.pulsar.PulsarProducer;
import org.apache.nifi.pulsar.pool.PulsarConsumerFactory;
import org.apache.nifi.pulsar.pool.PulsarProducerFactory;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.junit.Rule;
import org.mockito.ArgumentMatcher;
import org.mockito.Matchers;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

import static org.mockito.Mockito.*;

import java.util.Properties;

public class MockPulsarClientService extends AbstractControllerService implements PulsarClientPool {

	@Mock
	PulsarClient mockClient;
	
	@Mock
	Producer mockProducer;
	
	@Mock
	Consumer mockConsumer;
	
	@Rule public MockitoRule mockitoRule = MockitoJUnit.rule(); 
	
	public MockPulsarClientService() {
		if (mockProducer == null) {
			mockProducer = mock(Producer.class);
			try {
				when(mockProducer.send(Matchers.argThat(new ArgumentMatcher<byte[]>() {
				    @Override
				    public boolean matches(Object argument) {			        
				        return true;
				    }
				}))).thenReturn(null);
			} catch (PulsarClientException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		
		if (mockConsumer == null) {
			mockConsumer = mock(Consumer.class);
			
		}
		
		if (mockClient == null) {
			mockClient = mock(PulsarClient.class);
			try {
				when(mockClient.createProducer(anyString())).thenReturn(getMockProducer());
			} catch (PulsarClientException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		
	}
	

	public Producer getMockProducer() {		
		return mockProducer;
	}

	public PulsarClient getMockClient() {		
		return mockClient;
	}

	@Override
	public PulsarProducer getProducer(Properties props) throws PulsarClientException {
		String topic = props.getProperty(PulsarProducerFactory.TOPIC_NAME);
		return new PulsarProducer(mockClient.createProducer(topic), topic);
	}

	@Override
	public PulsarConsumer getConsumer(Properties props) throws PulsarClientException {
		String topic = props.getProperty(PulsarConsumerFactory.TOPIC_NAME);
		String subscription = props.getProperty(PulsarConsumerFactory.SUBSCRIPTION_NAME);
		return new PulsarConsumer(mockClient.subscribe(topic, subscription), topic, subscription);
	}

	@Override
	public void release(PulsarProducer producer) {
		// TODO Auto-generated method stub
		
	}


	@Override
	public void invalidate(PulsarProducer producer) {
		// TODO Auto-generated method stub
		
	}


	@Override
	public void release(PulsarConsumer consumer) {
		// TODO Auto-generated method stub
		
	}


	@Override
	public void invalidate(PulsarConsumer consumer) {
		// TODO Auto-generated method stub
		
	}
	
}
