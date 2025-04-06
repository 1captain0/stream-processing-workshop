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
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerde;

import java.util.Properties;

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
//    private TestOutputTopic<String, DiscountMilestoneEmail.DiscountMilestoneEmailConfirmation> outputTopic;
//    private TestOutputTopic<String, DiscountMilestoneEmail.StreamClass> outputTopicStream;
    private TestOutputTopic<String, DiscountMilestoneEmail.AggregatedStreamEnriched> outputTopic;
    private TestOutputTopic<String, Event> eventOutputTopic;

//    DiscountMilestoneEmail.AGGREGATED_STREAM_JSON_SERDE.deserializer().addTrustedPackages("org.improving.workshop.samples");


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

// use for end output topic
//        outputTopic = driver.createOutputTopic(
//                DiscountMilestoneEmail.OUTPUT_TOPIC,
//                stringDeserializer,
//                DiscountMilestoneEmail.DISCOUNT_MILESTONE_EVENT_JSON_SERDE.deserializer()
//        );

// intermediate output topic
//        outputTopicStream = driver.createOutputTopic(
//                DiscountMilestoneEmail.OUTPUT_TOPIC,
//                stringDeserializer,
//                DiscountMilestoneEmail.STREAM_CLASS_JSON_SERDE.deserializer()
//        );

        // intermediate output topic
//        outputTopicAggregatedStream = driver.createOutputTopic(
//                DiscountMilestoneEmail.OUTPUT_TOPIC,
//                stringDeserializer,
//                DiscountMilestoneEmail.AGGREGATED_STREAM_JSON_SERDE.deserializer()
//        );


// intermediate output topic
        outputTopic = driver.createOutputTopic(
                DiscountMilestoneEmail.OUTPUT_TOPIC,
                stringDeserializer,
                DiscountMilestoneEmail.AGGREGATED_STREAM_ENRICHED_JSON_SERDE.deserializer()
        );

        eventOutputTopic = driver.createOutputTopic(
                DiscountMilestoneEmail.EVENT_OUTPUT_TOPIC,
                stringDeserializer,
                Streams.SERDE_EVENT_JSON.deserializer()
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

    String eventId1 = "event-1";
    String eventId2 = "event-2";
    String eventId3 = "event-3";

    eventInputTopic.pipeInput(eventId1, new Event(eventId1, "artist-1", "venue-1", 5, "2025-11-21 00:07:06.973"));
    eventInputTopic.pipeInput(eventId2, new Event(eventId2, "artist-1", "venue-2", 10, "2025-04-24 06:49:41.862"));
    eventInputTopic.pipeInput(eventId3, new Event(eventId3, "artist-2", "venue-2", 15, "2025-10-24 06:49:41.862"));

    var eventOutputRecords = eventOutputTopic.readRecordsToList();
    assertEquals(3, eventOutputRecords.size());

    String streamId1 = "stream-1";
    String streamId2 = "stream-2";
    String streamId3 = "stream-3";
    String streamId4 = "stream-4";
    String streamId5 = "stream-5";
    String streamId6 = "stream-6";

    streamInputTopic.pipeInput(streamId1, new Stream(streamId1, "customer-1", "artist-1", "2"));
    streamInputTopic.pipeInput(streamId2, new Stream(streamId2, "customer-1", "artist-1", "5"));
    streamInputTopic.pipeInput(streamId5, new Stream(streamId5, "customer-1", "artist-2", "5"));
    streamInputTopic.pipeInput(streamId5, new Stream(streamId6, "customer-1", "artist-2", "3"));
    streamInputTopic.pipeInput(streamId3, new Stream(streamId3, "customer-2", "artist-1", "7"));
    streamInputTopic.pipeInput(streamId4, new Stream(streamId4, "customer-2", "artist-1", "3"));

    var outputRecords = outputTopic.readRecordsToList();
    assertEquals(3, outputRecords.size());

    }
}
