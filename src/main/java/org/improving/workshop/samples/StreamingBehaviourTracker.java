package org.improving.workshop.samples;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.improving.workshop.Streams;
import org.msse.demo.mockdata.music.event.Event;
import org.msse.demo.mockdata.music.ticket.Ticket;
import org.springframework.kafka.support.serializer.JsonSerde;
import java.util.LinkedHashMap;
import static org.apache.kafka.streams.state.Stores.persistentKeyValueStore;
import static org.improving.workshop.Streams.*;

@Slf4j
public class StreamingBehaviourTracker {

    public static final String OUTPUT_TOPIC = "kafka-workshop-streaming-vs-attending-match";
    public static final JsonSerde<LinkedHashMap<String, Long>> LINKED_HASH_MAP_JSON_SERDE
            = new JsonSerde<>(
            new TypeReference<LinkedHashMap<String, Long>>() {
            },
            new ObjectMapper()
                    .configure(DeserializationFeature.USE_LONG_FOR_INTS, true)
    );
    public static final JsonSerde<EventTicket> SERDE_EVENT_TICKET_JSON = new JsonSerde<>(EventTicket.class);
    public static final JsonSerde<MatchedArtist> MATCHED_ARTIST_SERDE =
            new JsonSerde<>(MatchedArtist.class);



    public static final String TOP_STREAMED_STORE = "top-streamed-artist";
    public static final String TOP_ATTENDED_STORE = "top-attended-artist";

    public static final JsonSerde<ArtistCount> ARTIST_COUNT_SERDE = new JsonSerde<>(ArtistCount.class);

    public static void main(final String[] args) {
        final StreamsBuilder builder = new StreamsBuilder();
        configureTopology(builder);
        startStreams(builder);
    }

    static void configureTopology(final StreamsBuilder builder) {
        // Step 1: Top Streamed Artist Per Customer (as KTable)
        KTable<String, ArtistCount> topStreamedArtist = builder
                .stream(TOPIC_DATA_DEMO_STREAMS, Consumed.with(Serdes.String(), SERDE_STREAM_JSON))
                .peek((streamId, stream) -> log.info("Stream Received: {}", stream))
                .groupBy(
                        (k, v) -> v.customerid() + "|" + v.artistid(),
                        Grouped.with(Serdes.String(), SERDE_STREAM_JSON)
                )
                .count(Materialized.as("stream-counts"))
                .toStream()
                .peek((key, count) -> log.info("[2] Stream Count -> Key: {}, Count: {}", key, count))
                .map((key, count) -> {
                    String[] parts = key.split("\\|");
                    return KeyValue.pair(parts[0], new ArtistCount(parts[0], parts[1], count));
                })
                .peek((customerId, artistCount) -> log.info("[3] ReKeyed Streamed: Key={}, Value={}", customerId, artistCount))
                .groupByKey(Grouped.with(Serdes.String(), ARTIST_COUNT_SERDE))
                .aggregate(
                        () -> new ArtistCount("", "", 0L),
                        (customerId, newCount, currentTop) ->
                                newCount.count > currentTop.count ? newCount : currentTop,
                        Materialized.<String, ArtistCount>as(persistentKeyValueStore(TOP_STREAMED_STORE))
                                .withKeySerde(Serdes.String())
                                .withValueSerde(ARTIST_COUNT_SERDE)
                );

        // Step 2: Build Events KTable
        KTable<String, Event> eventsTable = builder.table(
                TOPIC_DATA_DEMO_EVENTS,
                Materialized.<String, Event>as(persistentKeyValueStore("events-store"))
                        .withKeySerde(Serdes.String())
                        .withValueSerde(Streams.SERDE_EVENT_JSON)
        );

        // Step 3: Enrich Tickets with Events
        KStream<String, EventTicket> enrichedTickets = builder
                .stream(TOPIC_DATA_DEMO_TICKETS, Consumed.with(Serdes.String(), SERDE_TICKET_JSON))
                .peek((ticketId, ticket) -> log.info("[1] Ticket Received: {}", ticket))
                .selectKey((ticketId, ticket) -> ticket.eventid(), Named.as("rekey-by-eventid"))
                .join(eventsTable, EventTicket::new)
                .peek((eventId, et) -> log.info("[2] Joined EventTicket: {}", et));

        // Step 4: Count Tickets Per Customer+Artist
        KTable<String, Long> attendedCounts = enrichedTickets
                .selectKey((eventId, et) -> et.getTicket().customerid() + "|" + et.getEvent().artistid())
                .peek((key, value) -> log.info("[3] Grouped Key (Customer|Artist): {}, Value: {}", key, value))
                .groupByKey(Grouped.with(Serdes.String(), SERDE_EVENT_TICKET_JSON))
                .count(Materialized.<String, Long>as(persistentKeyValueStore("attended-counts-store"))
                        .withKeySerde(Serdes.String())
                        .withValueSerde(Serdes.Long()));

        // Step 5: Top Attended Artist Per Customer (as KTable)
        KTable<String, ArtistCount> topAttendedArtist = attendedCounts
                .toStream()
                .map((key, count) -> {
                    String[] parts = key.split("\\|");
                    String customerId = parts[0];
                    String artistId = parts[1];
                    return KeyValue.pair(customerId, new ArtistCount(customerId, artistId, count));
                })
                .peek((key, artistCount) -> log.info("[4] ReKeyed Attended: Key={}, Value={}", key, artistCount))
                .groupByKey(Grouped.with(Serdes.String(), ARTIST_COUNT_SERDE))
                .aggregate(
                        () -> new ArtistCount("", "", 0L),
                        (customerId, newCount, currentTop) ->
                                newCount.count > currentTop.count ? newCount : currentTop,
                        Materialized.<String, ArtistCount>as(persistentKeyValueStore(TOP_ATTENDED_STORE))
                                .withKeySerde(Serdes.String())
                                .withValueSerde(ARTIST_COUNT_SERDE)
                );

        // Step 6: Join Top Streamed and Top Attended Artists Per Customer
        KStream<String, MatchedArtist> matchedArtists = topStreamedArtist.join(
                        topAttendedArtist,
                        MatchedArtist::new
                )
                .toStream()
                .filter((customerId, match) ->
                        match != null &&
                                match.getTopStreamed() != null &&
                                match.getTopAttended() != null &&
                                match.getTopStreamed().getArtistId().equals(match.getTopAttended().getArtistId())
                )
                .peek((customerId, match) ->
                        log.info("[5] Matched Streamed and Attended -> Key: {}, Value: {}", customerId, match));


        // Step 7: Output Matched Artists to Final Sink Topic
        matchedArtists.to(OUTPUT_TOPIC, Produced.with(Serdes.String(), MATCHED_ARTIST_SERDE));

    }

    @Getter
    @NoArgsConstructor // ✅ Add this
    @AllArgsConstructor
    public static class EventTicket {
        private Ticket ticket;
        private Event event;
    }

    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    public static class ArtistCount {
        private String customerId;
        private String artistId;
        private Long count;
    }

    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    public static class MatchedArtist {
        private ArtistCount topStreamed;
        private ArtistCount topAttended;
    }

}
