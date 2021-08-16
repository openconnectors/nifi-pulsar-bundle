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
package org.apache.nifi.processors.pulsar.pubsub.sync;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.nifi.processors.pulsar.pubsub.ConsumePulsar;
import org.apache.nifi.processors.pulsar.pubsub.TestConsumePulsar;
import org.apache.nifi.util.MockFlowFile;
import org.apache.pulsar.client.api.PulsarClientException;
import org.junit.Test;

public class TestSyncConsumePulsar extends TestConsumePulsar {

    @Test
    public void nullMessageTest() throws PulsarClientException {
        when(mockClientService.getMockConsumer().receive(0, TimeUnit.SECONDS)).thenReturn(mockMessage).thenReturn(null);
        when(mockMessage.getData()).thenReturn(null);
        mockClientService.setMockMessage(mockMessage);

        runner.setProperty(ConsumePulsar.TOPICS, "foo");
        runner.setProperty(ConsumePulsar.SUBSCRIPTION_NAME, "bar");
        runner.setProperty(ConsumePulsar.SUBSCRIPTION_TYPE, "Exclusive");
        runner.run();
        runner.assertAllFlowFilesTransferred(ConsumePulsar.REL_SUCCESS);

        // Make sure no Flowfiles were generated
        List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(ConsumePulsar.REL_SUCCESS);
        assertEquals(0, flowFiles.size());

        verify(mockClientService.getMockConsumer(), atLeast(1)).acknowledgeCumulative(mockMessage);
    }

    @Test
    public void pulsarClientExceptionTest() throws PulsarClientException {
        when(mockClientService.getMockConsumer().receive()).thenThrow(PulsarClientException.class);

        runner.setProperty(ConsumePulsar.TOPICS, "foo");
        runner.setProperty(ConsumePulsar.SUBSCRIPTION_NAME, "bar");
        runner.run();
        runner.assertAllFlowFilesTransferred(ConsumePulsar.REL_SUCCESS);

        // Make sure no Flowfiles were generated
        List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(ConsumePulsar.REL_SUCCESS);
        assertEquals(0, flowFiles.size());

        verify(mockClientService.getMockConsumer(), times(0)).acknowledge(mockMessage);
    }

    @Test
    public void emptyMessageTest() throws PulsarClientException {
        when(mockClientService.getMockConsumer().receive(0, TimeUnit.SECONDS)).thenReturn(mockMessage).thenReturn(null);
        when(mockMessage.getData()).thenReturn("".getBytes());
        mockClientService.setMockMessage(mockMessage);

        runner.setProperty(ConsumePulsar.TOPICS, "foo");
        runner.setProperty(ConsumePulsar.SUBSCRIPTION_NAME, "bar");
        runner.setProperty(ConsumePulsar.SUBSCRIPTION_TYPE, "Exclusive");
        runner.run();
        runner.assertAllFlowFilesTransferred(ConsumePulsar.REL_SUCCESS);

        // Make sure no Flowfiles were generated
        List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(ConsumePulsar.REL_SUCCESS);
        assertEquals(0, flowFiles.size());

        verify(mockClientService.getMockConsumer(), atLeast(1)).acknowledgeCumulative(mockMessage);
    }

    @Test
    public void singleMessageTest() throws PulsarClientException {
        this.sendMessages("Mocked Message", "foo", "bar", false, 1);
    }

    @Test
    public void multipleMessagesTest() throws PulsarClientException {
        this.sendMessages("Mocked Message", "foo", "bar", false, 40);
    }

    @Test
    public final void batchMessageTest() throws PulsarClientException {
        this.batchMessages("Mocked Message", "foo", "bar", false, 400);
    }

    /*
     * Verify that the consumer gets closed.
     */
    @Test
    public void onStoppedTest() throws PulsarClientException {
        when(mockMessage.getData()).thenReturn("Mocked Message".getBytes());
        mockClientService.setMockMessage(mockMessage);

        runner.setProperty(ConsumePulsar.TOPICS, "foo");
        runner.setProperty(ConsumePulsar.SUBSCRIPTION_NAME, "bar");
        runner.setProperty(ConsumePulsar.CONSUMER_BATCH_SIZE, 1 + "");
        runner.run(1, true);
        runner.assertAllFlowFilesTransferred(ConsumePulsar.REL_SUCCESS);

        runner.assertQueueEmpty();

        // Verify that the consumer was closed
        verify(mockClientService.getMockConsumer(), times(1)).close();
    }
}
