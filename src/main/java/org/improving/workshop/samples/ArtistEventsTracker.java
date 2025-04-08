package org.improving.workshop.samples;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import org.apache.kafka.streams.kstream.*;
import org.improving.workshop.Streams;
import org.msse.demo.mockdata.music.artist.Artist;
import org.msse.demo.mockdata.music.event.Event;
import org.msse.demo.mockdata.music.ticket.Ticket;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

//import org.apache.kafka.streams.kstream.Suppressed;


//import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.springframework.kafka.support.serializer.JsonSerde;

import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

//import static java.util.Collections.reverseOrder;
//import static java.util.stream.Collectors.toMap;
import static org.apache.kafka.streams.state.Stores.persistentKeyValueStore;
import static org.improving.workshop.Streams.*;

@Slf4j
public class ArtistEventsTracker {



    // MUST BE PREFIXED WITH "kafka-workshop-"
    public static final String OUTPUT_TOPIC = "highest-lowest-artist-event";
    private static final Logger log = LoggerFactory.getLogger(ArtistEventsTracker.class);

    //public static final JsonSerde<SortedCounterMap> COUNTER_MAP_JSON_SERDE = new JsonSerde<>(SortedCounterMap.class);

    // Jackson is converting Value into Integer Not Long due to erasure,
    //public static final JsonSerde<LinkedHashMap<String, Long>> LINKED_HASH_MAP_JSON_SERDE = new JsonSerde<>(LinkedHashMap.class);
//    public static final JsonSerde<LinkedHashMap<String, Long>> LINKED_HASH_MAP_JSON_SERDE
//            = new JsonSerde<>(
//            new TypeReference<LinkedHashMap<String, Long>>() {
//            },
//            new ObjectMapper()
//                    .configure(DeserializationFeature.USE_LONG_FOR_INTS, true)
//    );
    public static final JsonSerde<ArtistEvent> ARTIST_EVENT_JSON_SERDE
            = new JsonSerde<>(
            new TypeReference<ArtistEvent>() {},
            new ObjectMapper().configure(DeserializationFeature.USE_LONG_FOR_INTS, true)
    );
    public static final JsonSerde<EventTicketAggregation> EVENT_TICKET_AGGREGATION_SERDE =
            new JsonSerde<>(EventTicketAggregation.class);



    /**
     * The Streams application as a whole can be launched like any normal Java application that has a `main()` method.
     */
    public static void main(final String[] args) {
        final StreamsBuilder builder = new StreamsBuilder();

        // configure the processing topology
        configureTopology(builder);

        // fire up the engines
        startStreams(builder);
    }

    static void configureTopology(final StreamsBuilder builder) {

        KTable<String, Event> eventsTable = builder
                .table(
                        TOPIC_DATA_DEMO_EVENTS,
                        Materialized
                                .<String, Event>as(persistentKeyValueStore("events"))
                                .withKeySerde(Serdes.String())
                                .withValueSerde(Streams.SERDE_EVENT_JSON)
                );
        KTable<String, Artist> artistTable = builder
                .table(
                        TOPIC_DATA_DEMO_ARTISTS,
                        Materialized
                                .<String, Artist>as(persistentKeyValueStore("artists"))
                                .withKeySerde(Serdes.String())
                                .withValueSerde(Streams.SERDE_ARTIST_JSON)
                );

        // capture the backside of the table to log a confirmation that the Event was received
        eventsTable.toStream().peek((key, event) -> log.info("Event '{}' registered for artist '{}' at venue '{}' with a capacity of {}.", key, event.artistid(), event.venueid(), event.capacity()));
        artistTable.toStream().peek((key, artist) -> log.info("Artist '{}' added  '{}' with name '{}' and genre {}.", key, artist.id(), artist.name(), artist.genre()));


        builder
                .stream(TOPIC_DATA_DEMO_TICKETS, Consumed.with(Serdes.String(), SERDE_TICKET_JSON))
                .peek((ticketId, ticketDetails) -> log.info("Ticket Details: {}", ticketDetails))
                .selectKey((ticketId, ticketRequest) -> ticketRequest.eventid(), Named.as("rekey-by-eventid"))
                .join(
                        eventsTable,
                        (eventId, ticket, event) -> new EventTicket(ticket, event)
                )
                .peek((key, value) -> log.info("Joined Event and Ticket: {}", value))
                .groupByKey()
                .aggregate(
                        // initializer
                        EventTicketAggregation::new,

                        // aggregator
                        (eventId, eventTicket,aggregation) -> {
                            // Increment the ticket count and keep the event details
                            if (!aggregation.initialized) {
                                aggregation.initialize(eventTicket.getEventTicket());
                            }
                            aggregation.incrementTicketCount();
                            return aggregation;

                        },
                        Materialized.with(Serdes.String(), EVENT_TICKET_AGGREGATION_SERDE)
                )

                // turn it back into a stream so that it can be produced to the OUTPUT_TOPIC
                .toStream()
                .selectKey((eventId, aggregation) -> aggregation.event.artistid())
                .groupByKey()
                .aggregate(
                        ArtistEventAggregation::new,

                        (artistId, eventTicket, aggregation) -> {
//                            double currentTurnout = eventTicket.getTurnout();
//
//                            // Check if we need to update best or worst event for the artist
//                            if (aggregation.bestEvent == null || currentTurnout >= aggregation.bestEvent.getTurnout()) {
//                                aggregation.bestEvent = eventTicket;  // Update the best event if current turnout is better
//                            }
//                            if (aggregation.worstEvent == null || currentTurnout < aggregation.worstEvent.getTurnout()) {
//                                aggregation.worstEvent = eventTicket;  // Update the worst event if current turnout is worse
//                            }
                            aggregation.updateEvent(eventTicket);

                            return aggregation;

                        },
                            Materialized
                                    .<String, ArtistEventAggregation>as(persistentKeyValueStore("artist-event-status"))
                                    .withKeySerde(Serdes.String())
                                    .withValueSerde(ArtistEventAggregation.SERDE)
                )
                .toStream()
                .peek((artistId, aggregation) -> {
                    log.info("Artist '{}' best event: '{}', worst event: '{}'",
                    artistId, aggregation.bestEvent.event.id(), aggregation.worstEvent.event.id());
                })
                .join(artistTable,
                        (artistId, ArtistEventAgg, artist) -> new ArtistEvent(ArtistEventAgg, artist)
                )
                // NOTE: when using ccloud, the topic must exist or 'auto.create.topics.enable' set to true (dedicated cluster required)
                .to(OUTPUT_TOPIC, Produced.with(Serdes.String(), ARTIST_EVENT_JSON_SERDE));
    }

    @Data
    public static class EventTicket {
        private Ticket ticket;
        private Event event;

        public EventTicket(Ticket ticket, Event event) {
            this.ticket = ticket;
            this.event = event;
        }

        public Event getEventTicket() {
            return event;
        }

    }

    @Data
    public static class ArtistEvent {
        private ArtistEventAggregation artistEvent;
        private Artist artist;

        public ArtistEvent(){

        }

        public ArtistEvent(ArtistEventAggregation artistEvent, Artist artist) {
            this.artistEvent = artistEvent;
            this.artist = artist;
        }

        public ArtistEventAggregation artistEvent() {
            return artistEvent;
        }

        public Artist getArtist(){
            return artist;
        }
    }

    @Data
    @AllArgsConstructor
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class EventTicketAggregation {
        private long ticketCount;
        private Event event;
        private boolean initialized;

        // Constructor to initialize with zero ticket count
        public EventTicketAggregation() {
            this.initialized = false;
        }

        public void initialize(Event event) {
            this.event = event;
            this.ticketCount = 0;
            this.initialized = true;
        }

        // Method to increment the ticket count
        public void incrementTicketCount() {
            this.ticketCount++;
        }

        public double getTurnout() {
            if (event == null || event.capacity() == 0) {
                return 0.0;  // Avoid division by zero
            }
            return (double) ticketCount / event.capacity();
        }

        public Event getTheEvent() {
            return event;
        }

    }
    @Data
    @AllArgsConstructor
    public static class ArtistEventAggregation {
        public Map<String, EventTicketAggregation> allEvents;
        public EventTicketAggregation bestEvent;
        public EventTicketAggregation worstEvent;

        public ArtistEventAggregation() {
            this.allEvents = new HashMap<>();
            this.bestEvent = null;
            this.worstEvent = null;
        }


        public void updateEvent(EventTicketAggregation newAgg) {
            String eventId = newAgg.event.id();
            allEvents.put(eventId, newAgg);

            // Recalculate best/worst from current state
            this.bestEvent = allEvents.values().stream()
                    .max(Comparator.comparingDouble(EventTicketAggregation::getTurnout))
                    .orElse(null);


            this.worstEvent = allEvents.values().stream()
                    .min(Comparator.comparingDouble(EventTicketAggregation::getTurnout))
                    .orElse(null);
        }

        // Serde for the aggregation object (you can implement it if necessary)
        public static final JsonSerde<ArtistEventAggregation> SERDE = new JsonSerde<>(ArtistEventAggregation.class);
    }


}