package com.ascentstream.demo;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;

/**
 * Unit test for simple App.
 */
public class PulsarDemoApplicationTest
    extends TestCase
{
    /**
     * Create the test case
     *
     * @param testName name of the test case
     */
    public PulsarDemoApplicationTest(String testName )
    {
        super( testName );
    }

    /**
     * @return the suite of tests being tested
     */
    public static Test suite()
    {
        return new TestSuite( PulsarDemoApplicationTest.class );
    }

    /**
     * Rigourous Test :-)
     */
    public void testApp()
    {
        assertTrue( true );
    }

    public void testPulsarClient() throws PulsarClientException {
        PulsarClient client = PulsarClient.builder()
                .serviceUrl("pulsar://brok0.as.com:6650,brok1.as.com:6650,brok2.as.com:6650")
                .build();
        System.out.println("Pulsar client init done.");


        Producer<byte[]> producer = client.newProducer()
                .topic("persistent://xuwei/ns-xuwei/topic-xuwei")
                .create();
        System.out.println("Pulsar producer init done.");


        Consumer<byte[]> consumer = client.newConsumer()
                .topic("persistent://xuwei/ns-xuwei/topic-xuwei")
                .subscriptionName("subscription-xuwei")
                .subscribe();
        System.out.println("Pulsar consumer init done.");

    }
}
