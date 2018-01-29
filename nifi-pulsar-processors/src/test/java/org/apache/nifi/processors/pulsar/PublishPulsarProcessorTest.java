package org.apache.nifi.processors.pulsar;

import java.io.UnsupportedEncodingException;
import java.util.HashMap;
import java.util.Map;

import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.apache.pulsar.client.api.PulsarClientException;
import org.junit.Before;
import org.junit.Test;

import static org.mockito.Mockito.*;

public class PublishPulsarProcessorTest extends AbstractPulsarProcessorTest {
	
	private TestRunner runner;
	private MockPulsarClientService pulsarClient;

    @Before
    public void init() throws InitializationException {
        runner = TestRunners.newTestRunner(PublishPulsar.class);
        pulsarClient = getPulsarClientService(runner);
    }
	
	@Test
	public void invalidTopicTest() throws UnsupportedEncodingException, PulsarClientException {
		
		runner.setProperty(PublishPulsar.TOPIC, "${topic}");
		
		final String content = "some content";
        Map<String, String> attributes = new HashMap<String, String> ();
        attributes.put(PublishPulsar.TOPIC.getName(), "");
        
		runner.enqueue(content.getBytes("UTF-8"), attributes );
        runner.run();
        runner.assertAllFlowFilesTransferred(PublishPulsar.REL_FAILURE);
        
        // Confirm that no Producer as created 
        verify(pulsarClient.getMockClient(), times(0)).createProducer(anyString());
	}
	
	@Test
	public void dynamicTopicTest() throws UnsupportedEncodingException, PulsarClientException {
		
		runner.setProperty(PublishPulsar.TOPIC, "${topic}");
		
		final String content = "some content";
        Map<String, String> attributes = new HashMap<String, String> ();
        attributes.put(PublishPulsar.TOPIC.getName(), "topic-b");
        
		runner.enqueue(content.getBytes("UTF-8"), attributes );
        runner.run();
        runner.assertAllFlowFilesTransferred(PublishPulsar.REL_SUCCESS);
        
        // Verify that we sent the data to topic-b.
        verify(pulsarClient.getMockClient(), times(1)).createProducer("topic-b");
	}

	@Test
    public void singleFlowFileTest() throws UnsupportedEncodingException, PulsarClientException {
		
		runner.setProperty(PublishPulsar.TOPIC, "my-topic");
	
		final String content = "some content";
        runner.enqueue(content.getBytes("UTF-8"));
        runner.run();
        runner.assertAllFlowFilesTransferred(PublishPulsar.REL_SUCCESS);

        final MockFlowFile outFile = runner.getFlowFilesForRelationship(PublishPulsar.REL_SUCCESS).get(0);
        outFile.assertContentEquals(content);
        
        // Verify that we sent the data to my-topic.
        verify(pulsarClient.getMockClient(), times(1)).createProducer("my-topic");
        
        // Verify that the send method on the producer was called with the expected content
        verify(pulsarClient.getMockProducer(), times(1)).send(content.getBytes());
	}
	
	@Test
	public void multipleFlowFilesTest() throws UnsupportedEncodingException, PulsarClientException {
		
		runner.setProperty(PublishPulsar.TOPIC, "my-topic");
		final String content = "some content";
		
		runner.enqueue(content.getBytes("UTF-8"));
		runner.run(1, false);
        runner.assertAllFlowFilesTransferred(PublishPulsar.REL_SUCCESS);
        
        // Verify that the send method on the producer was called with the expected content
        verify(pulsarClient.getMockProducer(), times(1)).send(content.getBytes());
	}

}
