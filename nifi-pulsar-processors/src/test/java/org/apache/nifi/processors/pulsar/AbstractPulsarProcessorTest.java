package org.apache.nifi.processors.pulsar;

import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.TestRunner;

public abstract class AbstractPulsarProcessorTest {

	protected MockPulsarClientService getPulsarClientService(TestRunner runner) throws InitializationException {
        final MockPulsarClientService pulsarClient = new MockPulsarClientService();
        runner.addControllerService("pulsarClient", pulsarClient);
        runner.enableControllerService(pulsarClient);
        runner.setProperty(PublishPulsar.PULSAR_CLIENT_SERVICE, "pulsarClient");
        return pulsarClient;
    }
}
