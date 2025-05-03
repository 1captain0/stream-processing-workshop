package org.improving.workshop.samples;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueStore;
import org.improving.workshop.Streams;
import org.msse.demo.mockdata.music.artist.Artist;
import org.msse.demo.mockdata.music.event.Event;
import org.msse.demo.mockdata.music.ticket.Ticket;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

//import org.apache.kafka.streams.kstream.Suppressed;


//import com.fasterxml.jackson.core.JsonParser;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.springframework.kafka.support.serializer.JsonSerde;

//import static java.util.Collections.reverseOrder;
//import static java.util.stream.Collectors.toMap;
import static org.apache.kafka.streams.state.Stores.persistentKeyValueStore;
import static org.improving.workshop.Streams.*;

@Slf4j
public class ArtistEventEnrich {



    // MUST BE PREFIXED WITH "kafka-workshop-"
    public static final String OUTPUT_TOPIC = "data-demo-etenriches";
    private static final Logger log = LoggerFactory.getLogger(ArtistEventEnrich.class);


    public static final JsonSerde<EventTicket> EVENT_TICKET_JSON_SERDE =
            new JsonSerde<>(EventTicket.class);

    public static final JsonSerde<ArtistEventEnrich.EventTicketAggregation> EVENT_TICKET_AGGREGATION_SERDE =
            new JsonSerde<>(ArtistEventEnrich.EventTicketAggregation.class);


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

        Materialized<String, EventTicketAggregation, KeyValueStore<Bytes, byte[]>> materialized =
                Materialized.<String, EventTicketAggregation, KeyValueStore<Bytes, byte[]>>with(Serdes.String(), EVENT_TICKET_AGGREGATION_SERDE)
                        .withCachingDisabled();

        // capture the backside of the table to log a confirmation that the Event was received
        //eventsTable.toStream().peek((key, event) -> log.info("Event '{}' registered for artist '{}' at venue '{}' with a capacity of {}.", key, event.artistid(), event.venueid(), event.capacity()));
        //artistTable.toStream().peek((key, artist) -> log.info("Artist '{}' added  '{}' with name '{}' and genre {}.", key, artist.id(), artist.name(), artist.genre()));


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
                                aggregation.initialize(eventTicket.getEvent());
                            }
                            aggregation.incrementTicketCount();
                            return aggregation;

                        },
                        materialized

                )

                // turn it back into a stream so that it can be produced to the OUTPUT_TOPIC
                .toStream()
                .peek((key, value) -> log.info("Event-id {}:,tickets: {}, turnout: {}", value.eventid,value.getCount(),value.getTurnout()))
                .to(OUTPUT_TOPIC, Produced.with(Serdes.String(), EVENT_TICKET_AGGREGATION_SERDE));
    }

    @Data
    public static class EventTicket {
        private Ticket ticket;
        private Event event;

        public EventTicket(){

        }

        public EventTicket(Ticket ticket, Event event) {
            this.ticket = ticket;
            this.event = event;
        }

        public Event getEvent() {
            return this.event;
        }

        public Ticket getTicket(){
            return this.ticket;
        }

        public static final JsonSerde<EventTicket> SERDE = new JsonSerde<>(EventTicket.class);

    }


    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class EventTicketAggregation {
        @JsonProperty("ticket_count")
        private long ticketCount;

        @JsonProperty("eventid")
        private String eventid;

        @JsonProperty("artistid")
        private String artistid;

//        @JsonProperty(value = "event", access = JsonProperty.Access.WRITE_ONLY)
//        private Event event;

        @JsonProperty("initialized")
        private boolean initialized;

        @JsonProperty("turnout")
        private double turnout;

        @JsonProperty("timestamp")
        private double timestamp;

        @JsonProperty("capacity")
        private double capacity;




        // Constructor to initialize with zero ticket count
        public EventTicketAggregation() {
            this.initialized = false;
        }

        public void initialize(Event event) {
            this.artistid = event.artistid();
            this.eventid = event.id();
            this.capacity = event.capacity();
            this.ticketCount = 0;
            this.initialized = true;
            this.turnout = 0.0;
            this.timestamp = System.currentTimeMillis();
        }

        // Method to increment the ticket count
        public void incrementTicketCount() {
            this.ticketCount++;
            if (this.eventid == null || this.capacity == 0) {
                return ;  // Avoid division by zero
            }
            this.turnout = (double) this.ticketCount / this.capacity;
            this.timestamp = System.currentTimeMillis();
        }

        @JsonIgnore
        public double getTurnout() {
            return this.turnout;
        }

        @JsonIgnore
        public double getCount() {
            return this.ticketCount;
        }

        @JsonIgnore
        public String getTheEvent() {
            return eventid;
        }


    }

}