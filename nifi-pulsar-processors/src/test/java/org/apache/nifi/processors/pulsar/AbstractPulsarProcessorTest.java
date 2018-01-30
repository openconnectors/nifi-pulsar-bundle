package org.apache.nifi.processors.pulsar;

import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.TestRunner;
import org.apache.pulsar.client.api.PulsarClient;
import org.mockito.Mock;

public abstract class AbstractPulsarProcessorTest {
	
	protected TestRunner runner;
	
    @Mock
	protected PulsarClient mockClient;

	protected void addPulsarClientService() throws InitializationException {
        final MockPulsarClientService pulsarClient = new MockPulsarClientService(mockClient);
        runner.addControllerService("pulsarClient", pulsarClient);
        runner.enableControllerService(pulsarClient);
        runner.setProperty(PublishPulsar.PULSAR_CLIENT_SERVICE, "pulsarClient");
    }
}
