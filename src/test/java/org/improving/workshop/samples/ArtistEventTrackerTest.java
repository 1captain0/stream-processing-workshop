package org.improving.workshop.samples;

import org.apache.kafka.streams.*;
import org.apache.kafka.streams.test.TestRecord;
import org.improving.workshop.Streams;
import org.improving.workshop.utils.DataFaker;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.msse.demo.mockdata.music.artist.Artist;
import org.msse.demo.mockdata.music.event.Event;
import org.msse.demo.mockdata.music.ticket.Ticket;
import org.improving.workshop.samples.ArtistEventsTracker.ArtistEvent;
import org.apache.kafka.common.serialization.Serdes;

import java.util.ArrayList;
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
        String eventId4 = "event-4";
        String eventId5 = "event-5";
        String artistId = "artist-1";
        String artistId2 = "artist-2";

        // Create sample event and ticket
        Event event1 = new Event(eventId1, artistId, "venue-1", 10, "2025-03-27");
        Event event2 = new Event(eventId2, artistId, "venue-2", 10, "2025-06-27");
        Event event3 = new Event(eventId3, artistId, "venue-1", 10, "2025-08-27");
        Event event4 = new Event(eventId4, artistId2, "venue-1", 10, "2025-03-27");
        Event event5 = new Event(eventId5, artistId2, "venue-2", 10, "2025-06-27");
        //Event event6 = new Event(eventId3, artistId, "venue-1", 10, "2025-08-27");
        Artist artist = new Artist(artistId, "Eminem", "Rap");
        Artist artist2 = new Artist(artistId2, "George Ezra", "Pop");
        //Ticket ticket = new Ticket("ticket-1", eventId);

        // Push sample event and ticket data into the input topics
        eventInputTopic.pipeInput(eventId1, event1);
        eventInputTopic.pipeInput(eventId2, event2);
        eventInputTopic.pipeInput(eventId3, event3);
        eventInputTopic.pipeInput(eventId4, event4);
        eventInputTopic.pipeInput(eventId5, event5);
        artistInputTopic.pipeInput(artistId, artist);
        artistInputTopic.pipeInput(artistId2, artist2);
        ticketInputTopic.pipeInput(DataFaker.TICKETS.generate("customer-1", eventId1));
        ticketInputTopic.pipeInput(DataFaker.TICKETS.generate("customer-2", eventId2));
        ticketInputTopic.pipeInput(DataFaker.TICKETS.generate("customer-3", eventId1));
        ticketInputTopic.pipeInput(DataFaker.TICKETS.generate("customer-1", eventId5));
        ticketInputTopic.pipeInput(DataFaker.TICKETS.generate("customer-2", eventId4));
        ticketInputTopic.pipeInput(DataFaker.TICKETS.generate("customer-3", eventId4));
        ticketInputTopic.pipeInput(DataFaker.TICKETS.generate("customer-4", eventId3));
        ticketInputTopic.pipeInput(DataFaker.TICKETS.generate("customer-5", eventId3));
        ticketInputTopic.pipeInput(DataFaker.TICKETS.generate("customer-6", eventId3));

        ticketInputTopic.pipeInput(DataFaker.TICKETS.generate("customer-9", eventId3));

        ticketInputTopic.pipeInput(DataFaker.TICKETS.generate("customer-7", eventId2));
        ticketInputTopic.pipeInput(DataFaker.TICKETS.generate("customer-8", eventId2));
        ticketInputTopic.pipeInput(DataFaker.TICKETS.generate("customer-4", eventId5));
        ticketInputTopic.pipeInput(DataFaker.TICKETS.generate("customer-5", eventId5));
        ticketInputTopic.pipeInput(DataFaker.TICKETS.generate("customer-6", eventId5));

        var outputRecords = outputTopic.readRecordsToList();
        // Read output records from the output topic

        List<TestRecord<String, ArtistEvent>> artist1Records = new ArrayList<>();
        List<TestRecord<String, ArtistEvent>> artist2Records = new ArrayList<>();

        for (TestRecord<String, ArtistEvent> record : outputRecords) {
            if(record.key().equals("artist-1")){
                artist1Records.add(record);
            }
            else{
                artist2Records.add(record);
            }
            System.out.println(record.key()+" "+record.value().artistEvent().bestEvent.getTheEvent().id()+" "+record.value().artistEvent().worstEvent.getTheEvent().id());
        }

        System.out.println(artist1Records.getLast());
        System.out.println(artist2Records.getLast());

        assertEquals("event-1",artist1Records.getFirst().value().artistEvent().bestEvent.getTheEvent().id());
        assertEquals("event-1",artist1Records.getFirst().value().artistEvent().worstEvent.getTheEvent().id());
        assertEquals("event-3",artist1Records.getLast().value().artistEvent().bestEvent.getTheEvent().id());
        assertEquals("event-1",artist1Records.getLast().value().artistEvent().worstEvent.getTheEvent().id());

        assertEquals("event-5",artist2Records.getFirst().value().artistEvent().bestEvent.getTheEvent().id());
        assertEquals("event-5",artist2Records.getFirst().value().artistEvent().worstEvent.getTheEvent().id());
        assertEquals("event-5",artist2Records.getLast().value().artistEvent().bestEvent.getTheEvent().id());
        assertEquals("event-4",artist2Records.getLast().value().artistEvent().worstEvent.getTheEvent().id());

    }

}