package org.improving.workshop.samples;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.improving.workshop.Streams;
import org.improving.workshop.utils.DataFaker;
import org.junit.jupiter.api.*;
import org.msse.demo.mockdata.music.event.Event;
import org.msse.demo.mockdata.music.stream.Stream;
import org.msse.demo.mockdata.music.ticket.Ticket;

import java.time.Duration;
import java.util.List;

public class StreamingBehaviourTrackerTest {

    private TopologyTestDriver driver;

    private final Serde<String> stringSerde = Serdes.String();

    private TestInputTopic<String, Stream> streamInputTopic;
    private TestInputTopic<String, Event> eventInputTopic;
    private TestInputTopic<String, Ticket> ticketInputTopic;

    private TestOutputTopic<String, StreamingBehaviourTracker.MatchedArtist> matchOutputTopic;

    @BeforeEach
    void setup() {
        StreamingBehaviourTracker.ARTIST_COUNT_SERDE.deserializer().addTrustedPackages("org.improving.workshop.samples");
        StreamingBehaviourTracker.MATCHED_ARTIST_SERDE.deserializer().addTrustedPackages("org.improving.workshop.samples");

        StreamsBuilder builder = new StreamsBuilder();
        StreamingBehaviourTracker.configureTopology(builder);
        driver = new TopologyTestDriver(builder.build(), Streams.buildProperties());

        streamInputTopic = driver.createInputTopic(
                Streams.TOPIC_DATA_DEMO_STREAMS,
                stringSerde.serializer(),
                Streams.SERDE_STREAM_JSON.serializer()
        );

        eventInputTopic = driver.createInputTopic(
                Streams.TOPIC_DATA_DEMO_EVENTS,
                stringSerde.serializer(),
                Streams.SERDE_EVENT_JSON.serializer()
        );

        ticketInputTopic = driver.createInputTopic(
                Streams.TOPIC_DATA_DEMO_TICKETS,
                stringSerde.serializer(),
                Streams.SERDE_TICKET_JSON.serializer()
        );

        matchOutputTopic = driver.createOutputTopic(
                StreamingBehaviourTracker.OUTPUT_TOPIC,
                stringSerde.deserializer(),
                StreamingBehaviourTracker.MATCHED_ARTIST_SERDE.deserializer()
        );
    }

    @AfterEach
    void cleanup() {
        driver.close();
    }

    @Test
    @DisplayName("should match top streamed and attended artist")
    void matchTopStreamedAndAttendedArtist() {
        String streamId1 = "stream-1";
        String streamId2 = "stream-2";
        String streamId3 = "stream-3";
        String streamId4 = "stream-4";
        String streamId5 = "stream-5";

        String eventId1 = "event-1";
        String eventId2 = "event-2";
        String eventId3 = "event-3";

        Event event1 = new Event(eventId1, "artist-1", "venue-1", 10, "2025-03-27");
        Event event2 = new Event(eventId2, "artist-2", "venue-2", 10, "2025-06-27");
        Event event3 = new Event(eventId3, "artist-1", "venue-1", 10, "2025-08-27");

        streamInputTopic.pipeInput(streamId1, new Stream(streamId1, "customer-1", "artist-1", "2"));
        streamInputTopic.pipeInput(streamId2, new Stream(streamId2, "customer-1", "artist-1", "5"));
        streamInputTopic.pipeInput(streamId5, new Stream(streamId5, "customer-1", "artist-1", "5"));
        streamInputTopic.pipeInput(streamId3, new Stream(streamId3, "customer-2", "artist-1", "7"));
        streamInputTopic.pipeInput(streamId4, new Stream(streamId4, "customer-2", "artist-1", "3"));

        eventInputTopic.pipeInput(eventId1, event1);
        eventInputTopic.pipeInput(eventId2, event2);
        eventInputTopic.pipeInput(eventId3, event3);

        ticketInputTopic.pipeInput(DataFaker.TICKETS.generate("customer-1", eventId1));
        ticketInputTopic.pipeInput(DataFaker.TICKETS.generate("customer-2", eventId2));
        ticketInputTopic.pipeInput(DataFaker.TICKETS.generate("customer-3", eventId1));
        ticketInputTopic.pipeInput(DataFaker.TICKETS.generate("customer-4", eventId3));
        ticketInputTopic.pipeInput(DataFaker.TICKETS.generate("customer-5", eventId3));
        ticketInputTopic.pipeInput(DataFaker.TICKETS.generate("customer-6", eventId3));
        ticketInputTopic.pipeInput(DataFaker.TICKETS.generate("customer-7", eventId2));
        ticketInputTopic.pipeInput(DataFaker.TICKETS.generate("customer-8", eventId2));
        ticketInputTopic.pipeInput(DataFaker.TICKETS.generate("customer-9", eventId3));

        driver.advanceWallClockTime(Duration.ofSeconds(1));

        List<KeyValue<String, StreamingBehaviourTracker.MatchedArtist>> matches =
                matchOutputTopic.readKeyValuesToList();

        Assertions.assertFalse(matches.isEmpty(), "Expected Matched Records");

        for (KeyValue<String, StreamingBehaviourTracker.MatchedArtist> kv : matches) {
            String customerId = kv.key;
            StreamingBehaviourTracker.MatchedArtist match = kv.value;

            System.out.println("\u2714 Matched customer: " + customerId + " -> " + match);

            Assertions.assertEquals(
                    match.getTopStreamed().getArtistId(),
                    match.getTopAttended().getArtistId(),
                    "Artist IDs should match"
            );

            Assertions.assertNotNull(match.getTopStreamed().getCount(), "Stream count should not be null");
            Assertions.assertNotNull(match.getTopAttended().getCount(), "Attend count should not be null");
        }
    }

    @Test
    @DisplayName("should not emit match when streamed and attended artists differ")
    void noMatchWhenArtistsDiffer() {
        streamInputTopic.pipeInput("stream-1", new Stream("stream-1", "customer-1", "artist-1", "3"));
        streamInputTopic.pipeInput("stream-2", new Stream("stream-2", "customer-1", "artist-1", "5"));
        streamInputTopic.pipeInput("stream-3", new Stream("stream-3", "customer-1", "artist-1", "7"));

        streamInputTopic.pipeInput("stream-4", new Stream("stream-4", "customer-2", "artist-2", "2"));
        streamInputTopic.pipeInput("stream-5", new Stream("stream-5", "customer-2", "artist-2", "4"));

        Event event1 = new Event("event-1", "artist-2", "venue-x", 100, "2025-01-01");
        Event event2 = new Event("event-2", "artist-1", "venue-y", 100, "2025-01-02");

        eventInputTopic.pipeInput("event-1", event1);
        eventInputTopic.pipeInput("event-2", event2);

        ticketInputTopic.pipeInput(DataFaker.TICKETS.generate("customer-1", "event-1"));
        ticketInputTopic.pipeInput(DataFaker.TICKETS.generate("customer-2", "event-2"));

        Assertions.assertTrue(matchOutputTopic.isEmpty(), "Expected no matches when streamed and attended artists differ");
    }
}
