package org.improving.workshop.samples;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.*;
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

import java.util.List;
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
    private TestOutputTopic<String, DiscountMilestoneEmail.FinalEnrichedOutput> finalEnrichedOutputTopic;
    private TestOutputTopic<String, Event> eventOutputTopic;

    @BeforeEach
    void setup() {
        // instantiate new builder
        StreamsBuilder streamsBuilder = new StreamsBuilder();

        // build the topology
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

        finalEnrichedOutputTopic = driver.createOutputTopic(
                DiscountMilestoneEmail.OUTPUT_TOPIC,
                stringDeserializer,
                DiscountMilestoneEmail.FINAL_ENRICHED_JSON_SERDE.deserializer()
        );

        eventOutputTopic = driver.createOutputTopic(
                DiscountMilestoneEmail.EVENT_OUTPUT_TOPIC,
                stringDeserializer,
                Streams.SERDE_EVENT_JSON.deserializer()
        );
    }

    @AfterEach
    void cleanup() {
        driver.close();
    }

    @Test
    @DisplayName("confirm milestone discounts")
    void confirmMilestoneDiscount() {

        var cust1 = new Customer("customer-1", "PREMIUM", "M", "John", "Steven", "James", "JSJ", "", "", "1989-01-20", "2022-01-02");
        var cust2 = new Customer("customer-2", "PREMIUM", "M", "Jane", "Jo", "James", "JJJ", "", "", "1990-01-20", "2022-01-02");

        customerInputTopic.pipeKeyValueList(List.of(
                new KeyValue<>(cust1.id(), cust1),
                new KeyValue<>(cust2.id(), cust2)
        ));

        emailInputTopic.pipeInput("email-1", new Email("email-1", "customer-1", "customer1@gmail.com"));
        emailInputTopic.pipeInput("email-2", new Email("email-2", "customer-2", "customer2@gmail.com"));

        String eventId1 = "event-1";
        String eventId2 = "event-2";
        String eventId3 = "event-3";
        String eventId4 = "event-4";

        eventInputTopic.pipeInput(eventId1, new Event(eventId1, "artist-1", "venue-1", 5, "2025-11-21 00:07:06.973"));
        eventInputTopic.pipeInput(eventId2, new Event(eventId4, "artist-1", "venue-4", 20, "2025-06-24 06:49:41.862"));
        eventInputTopic.pipeInput(eventId2, new Event(eventId2, "artist-1", "venue-2", 10, "2025-04-24 06:49:41.862"));
        eventInputTopic.pipeInput(eventId3, new Event(eventId3, "artist-2", "venue-2", 15, "2025-10-24 06:49:41.862"));

        var eventOutputRecords = eventOutputTopic.readRecordsToList();
        assertEquals(4, eventOutputRecords.size());

        // For customer-1 with artist-1, we send four streams (milestones at 2 and 4).
        streamInputTopic.pipeInput("stream-1", new Stream("stream-1", "customer-1", "artist-1", "2"));
        streamInputTopic.pipeInput("stream-2", new Stream("stream-2", "customer-1", "artist-1", "5"));
        streamInputTopic.pipeInput("stream-5", new Stream("stream-5", "customer-1", "artist-1", "5"));
        streamInputTopic.pipeInput("stream-6", new Stream("stream-6", "customer-1", "artist-1", "3"));
        // For customer-2 with artist-1, we send two streams (milestone at 2).
        streamInputTopic.pipeInput("stream-3", new Stream("stream-3", "customer-2", "artist-1", "7"));
        streamInputTopic.pipeInput("stream-4", new Stream("stream-4", "customer-2", "artist-1", "3"));

        var outputRecords = finalEnrichedOutputTopic.readRecordsToList();
        assertEquals(3, outputRecords.size());

        DiscountMilestoneEmail.FinalEnrichedOutput cust1RecordMilestone2 = null;
        DiscountMilestoneEmail.FinalEnrichedOutput cust1RecordMilestone4 = null;
        DiscountMilestoneEmail.FinalEnrichedOutput cust2RecordMilestone2 = null;

        for (TestRecord<String, DiscountMilestoneEmail.FinalEnrichedOutput> record : outputRecords) {
            if ("customer-1".equals(record.key())) {
                if (record.value().getTotalStreamCount() == 2) {
                    cust1RecordMilestone2 = record.value();
                } else if (record.value().getTotalStreamCount() == 4) {
                    cust1RecordMilestone4 = record.value();
                }
            } else if ("customer-2".equals(record.key()) && record.value().getTotalStreamCount() == 2) {
                cust2RecordMilestone2 = record.value();
            }
        }
        assertNotNull(cust1RecordMilestone2, "Customer 1 milestone at 2 streams is missing");
        assertNotNull(cust1RecordMilestone4, "Customer 1 milestone at 4 streams is missing");
        assertNotNull(cust2RecordMilestone2, "Customer 2 milestone at 2 streams is missing");

        // test for the values that are the earliest event for the artist
        DiscountMilestoneEmail.FinalEnrichedOutput enrichedOutput = outputRecords.get(0).value();
        assertEquals("2025-04-24 06:49:41.862", enrichedOutput.getEventDate(),
                "The earliest event date does not match the expected value");
    }
}
