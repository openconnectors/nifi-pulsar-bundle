package org.apache.nifi.processors.pulsar;

import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.pulsar.PulsarClientService;
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

public class MockPulsarClientService extends AbstractControllerService implements PulsarClientService {

	@Mock
	PulsarClient mockClient;
	
	@Mock
	Producer mockProducer;
	
	@Rule public MockitoRule mockitoRule = MockitoJUnit.rule(); 
	
	@Override
	public PulsarClient getClient() {
		
		try {
			mockClient = mock(PulsarClient.class);
			mockProducer = mock(Producer.class);
			
			when(mockClient.createProducer(anyString())).thenReturn(mockProducer);
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
		return mockClient;
	}
	
	public Producer getMockProducer() {
		return mockProducer;
	}

	public PulsarClient getMockClient() {
		return mockClient;
	}
	
}
