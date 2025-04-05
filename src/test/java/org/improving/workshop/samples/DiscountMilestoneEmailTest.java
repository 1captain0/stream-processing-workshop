package org.improving.workshop.samples;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.test.TestRecord;
import org.improving.workshop.Streams;
import org.improving.workshop.utils.DataFaker;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.msse.demo.mockdata.customer.email.Email;
import org.msse.demo.mockdata.customer.profile.Customer;
import org.msse.demo.mockdata.music.event.Event;
import org.msse.demo.mockdata.music.stream.Stream;
import org.msse.demo.mockdata.music.ticket.Ticket;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class DiscountMilestoneEmailTest {

    private final static Serializer<String> stringSerializer = Serdes.String().serializer();
    private final static Deserializer<String> stringDeserializer = Serdes.String().deserializer();

    private TopologyTestDriver driver;

    // inputs
    private TestInputTopic<String, Event> eventInputTopic;
    private TestInputTopic<String, Stream> streamInputTopic;
    private TestInputTopic<String, Customer> customerInputTopic;
    private TestInputTopic<String, Email> emailInputTopic;

    // outputs
    private TestOutputTopic<String, DiscountMilestoneEmail.DiscountMilestoneEmailConfirmation> outputTopic;
    private TestOutputTopic<String, DiscountMilestoneEmail.StreamClass> outputTopicStream;

    @BeforeEach
    void setup() {
        // instantiate new builder
        StreamsBuilder streamsBuilder = new StreamsBuilder();

        // build the CustomerStreamCount topology (by reference)
        DiscountMilestoneEmail.configureTopology(streamsBuilder);

        // build the TopologyTestDriver
        driver = new TopologyTestDriver(streamsBuilder.build(), Streams.buildProperties());

        eventInputTopic = driver.createInputTopic(
                Streams.TOPIC_DATA_DEMO_EVENTS,
                stringSerializer,
                Streams.SERDE_EVENT_JSON.serializer()
        );

        streamInputTopic = driver.createInputTopic(
                Streams.TOPIC_DATA_DEMO_STREAMS,
                stringSerializer,
                Streams.SERDE_STREAM_JSON.serializer()
        );

        customerInputTopic = driver.createInputTopic(
                Streams.TOPIC_DATA_DEMO_CUSTOMERS,
                stringSerializer,
                Streams.SERDE_CUSTOMER_JSON.serializer()
        );

        emailInputTopic = driver.createInputTopic(
                Streams.TOPIC_DATA_DEMO_EMAILS,
                stringSerializer,
                Streams.SERDE_EMAIL_JSON.serializer()
        );

//        outputTopic = driver.createOutputTopic(
//                DiscountMilestoneEmail.OUTPUT_TOPIC,
//                stringDeserializer,
//                DiscountMilestoneEmail.DISCOUNT_MILESTONE_EVENT_JSON_SERDE.deserializer()
//        );

        outputTopicStream = driver.createOutputTopic(
                DiscountMilestoneEmail.OUTPUT_TOPIC,
                stringDeserializer,
                DiscountMilestoneEmail.STREAM_CLASS_JSON_SERDE.deserializer()
        );
    }

    @AfterEach
    void cleanup() {
        // if this close doesn't run (test fails halfway through), subsequent tests may start on old state
        // run the test and let it cleanup, then run the test again.
        driver.close();
    }

    @Test
    @DisplayName("confirm milestone discounts")
    void confirmMilestoneDiscount() {
        // Test case goes here
    String streamId1 = "stream-1";
    String streamId2 = "stream-2";


    streamInputTopic.pipeInput(streamId1, new Stream(streamId1, "customer-1", "artist-1", "10"));
    streamInputTopic.pipeInput(streamId2, new Stream(streamId2, "customer-1", "artist-1", "5"));


    var outputRecords = outputTopicStream.readRecordsToList();
    assertEquals(2, outputRecords.size());

    }
}
