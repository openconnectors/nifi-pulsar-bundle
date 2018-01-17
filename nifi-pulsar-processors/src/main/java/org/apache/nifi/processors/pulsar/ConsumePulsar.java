package org.apache.nifi.processors.pulsar;

import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.pulsar.PulsarClientService;

public class ConsumePulsar extends AbstractPulsarProcessor {

	@Override
	public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
		
		final PulsarClientService pulsarClientService = context.getProperty(PULSAR_CLIENT_SERVICE)
        		.asControllerService(PulsarClientService.class);
		
        if (pulsarClientService == null) {
            context.yield();
            return;
        }

	}

}
