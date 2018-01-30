package org.apache.nifi.processors.pulsar;

import java.util.Properties;

import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.pulsar.PulsarClientPool;
import org.apache.nifi.pulsar.PulsarConsumer;
import org.apache.nifi.pulsar.PulsarProducer;
import org.apache.nifi.pulsar.pool.PulsarConsumerFactory;
import org.apache.nifi.pulsar.pool.PulsarProducerFactory;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.mockito.Mock;

public class MockPulsarClientService extends AbstractControllerService  implements PulsarClientPool {

	@Mock
	PulsarClient mockClient;
	
	public MockPulsarClientService(PulsarClient client) {
		this.mockClient = client;
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
	public void release(PulsarConsumer consumer) {
		// TODO Auto-generated method stub

	}

	@Override
	public void invalidate(PulsarConsumer consumer) {
		consumer.close();
	}

	@Override
	public void release(PulsarProducer producer) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void invalidate(PulsarProducer producer) {
		producer.close();
	}

}
