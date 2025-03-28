package org.improving.workshop.samples;

import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.test.TestRecord;
import org.improving.workshop.Streams;
import org.improving.workshop.utils.DataFaker;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestReporter;
import org.msse.demo.mockdata.music.artist.Artist;
import org.msse.demo.mockdata.music.event.Event;
import org.msse.demo.mockdata.music.ticket.Ticket;
import org.improving.workshop.samples.ArtistEventsTracker.ArtistEvent;
import org.apache.kafka.common.serialization.Serdes;
import org.springframework.kafka.support.serializer.JsonSerde;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class ArtistEventsTrackerTest {

    private TopologyTestDriver driver;
    private TestInputTopic<String, Event> eventInputTopic;
    private TestInputTopic<String, Ticket> ticketInputTopic;
    private TestInputTopic<String, Artist> artistInputTopic;
    private TestOutputTopic<String, ArtistEventsTracker.ArtistEvent> outputTopic;

    @BeforeEach
    void setUp() {
        // Initialize the TopologyTestDriver
        var streamsBuilder = new StreamsBuilder();
        ArtistEventsTracker.configureTopology(streamsBuilder);
        driver = new TopologyTestDriver(streamsBuilder.build(), Streams.buildProperties());

        // Set up the input topics for Event and Ticket
        eventInputTopic = driver.createInputTopic(
                Streams.TOPIC_DATA_DEMO_EVENTS,
                Serdes.String().serializer(),
                Streams.SERDE_EVENT_JSON.serializer()
        );

        ticketInputTopic = driver.createInputTopic(
                Streams.TOPIC_DATA_DEMO_TICKETS,
                Serdes.String().serializer(),
                Streams.SERDE_TICKET_JSON.serializer()
        );
        eventInputTopic = driver.createInputTopic(
                Streams.TOPIC_DATA_DEMO_EVENTS,
                Serdes.String().serializer(),
                Streams.SERDE_EVENT_JSON.serializer()
        );

        artistInputTopic = driver.createInputTopic(
                Streams.TOPIC_DATA_DEMO_ARTISTS,
                Serdes.String().serializer(),
                Streams.SERDE_ARTIST_JSON.serializer()
        );

        // Set up the output topic to capture ArtistEvent output
        outputTopic = driver.createOutputTopic(
                ArtistEventsTracker.OUTPUT_TOPIC,
                Serdes.String().deserializer(),
                ArtistEventsTracker.ARTIST_EVENT_JSON_SERDE.deserializer()
        );
    }

    @Test
    @DisplayName("Verify Artist Events Output")
    void verifyArtistEventsOutput() {
        // Example input data
        String eventId1 = "event-1";
        String eventId2 = "event-2";
        String eventId3 = "event-3";
        String artistId = "artist-1";

        // Create sample event and ticket
        Event event1 = new Event(eventId1, artistId, "venue-1", 10, "2025-03-27");
        Event event2 = new Event(eventId2, artistId, "venue-2", 10, "2025-06-27");
        Event event3 = new Event(eventId3, artistId, "venue-1", 10, "2025-08-27");
        Artist artist = new Artist(artistId, "Eminem", "Rap");
        //Ticket ticket = new Ticket("ticket-1", eventId);

        // Push sample event and ticket data into the input topics
        eventInputTopic.pipeInput(eventId1, event1);
        eventInputTopic.pipeInput(eventId2, event2);
        eventInputTopic.pipeInput(eventId3, event3);
        artistInputTopic.pipeInput(artistId, artist);
        ticketInputTopic.pipeInput(DataFaker.TICKETS.generate("customer-1", eventId1));
        ticketInputTopic.pipeInput(DataFaker.TICKETS.generate("customer-2", eventId2));
        ticketInputTopic.pipeInput(DataFaker.TICKETS.generate("customer-3", eventId1));
        ticketInputTopic.pipeInput(DataFaker.TICKETS.generate("customer-4", eventId3));
        ticketInputTopic.pipeInput(DataFaker.TICKETS.generate("customer-5", eventId3));
        ticketInputTopic.pipeInput(DataFaker.TICKETS.generate("customer-6", eventId3));
        ticketInputTopic.pipeInput(DataFaker.TICKETS.generate("customer-7", eventId2));
        ticketInputTopic.pipeInput(DataFaker.TICKETS.generate("customer-8", eventId2));
        ticketInputTopic.pipeInput(DataFaker.TICKETS.generate("customer-9", eventId3));
        var outputRecords = outputTopic.readRecordsToList();
        // Read output records from the output topic
        TestRecord<String, ArtistEvent> confirmation = outputRecords.getLast();

        // Ensure at least one record is output
        assertEquals(9, outputRecords.size());

        // Check if the ArtistEvent is properly formed
        TestRecord<String, ArtistEventsTracker.ArtistEvent> outputRecord = outputRecords.getLast();
        assertNotNull(outputRecord.value());

        // Assert key is correct (artist ID)
        assertEquals(artistId, outputRecord.key());

        // Check ArtistEvent content
        ArtistEvent artistEvent = outputRecord.value();
        assertNotNull(artistEvent.getArtist());
        assertNotNull(artistEvent.artistEvent());
        assertEquals(artistId, artistEvent.getArtist().id());

        // Additional assertions for best and worst events
        assertNotNull(artistEvent.artistEvent().bestEvent);
//        System.out.println(artistEvent.artistEvent());
//        System.out.println(artistEvent.artistEvent().bestEvent.getTurnout());
        assertNotNull(artistEvent.artistEvent().worstEvent);
//        System.out.println(artistEvent.artistEvent().bestEvent.getTheEvent().id());
//        System.out.println(artistEvent.artistEvent().worstEvent.getTurnout());
        System.out.println(outputRecord);
    }
}