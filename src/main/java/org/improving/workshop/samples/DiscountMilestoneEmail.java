package org.improving.workshop.samples;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.improving.workshop.Streams;
import org.msse.demo.mockdata.music.event.Event;
import org.msse.demo.mockdata.music.stream.Stream;
import org.msse.demo.mockdata.music.event.Event;
import org.apache.kafka.streams.KeyValue;
import org.msse.demo.mockdata.music.ticket.Ticket;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.support.serializer.JsonSerde;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import java.time.LocalDate;


import java.util.UUID;

import static org.apache.kafka.streams.state.Stores.persistentKeyValueStore;
import static org.improving.workshop.Streams.*;

public class DiscountMilestoneEmail {

    public static final String OUTPUT_TOPIC = "kafka-workshop-discount-milestone-email";
    public static final String EVENT_OUTPUT_TOPIC = "kafka-workshop-discount-milestone-email";


    public static final JsonSerde<DiscountMilestoneEmail.DiscountMilestoneEmailConfirmation> DISCOUNT_MILESTONE_EVENT_JSON_SERDE
        = new JsonSerde<>(DiscountMilestoneEmailConfirmation.class);

    // stream class for test
    public static final JsonSerde<DiscountMilestoneEmail.StreamClass> STREAM_CLASS_JSON_SERDE
            = new JsonSerde<>(StreamClass.class);

    public static final JsonSerde<DiscountMilestoneEmail.AggregatedStreamEnriched> AGGREGATED_STREAM_ENRICHED_JSON_SERDE =
            new JsonSerde<>(DiscountMilestoneEmail.AggregatedStreamEnriched.class);

    public static final JsonSerde<DiscountMilestoneEmail.AggregatedStream> AGGREGATED_STREAM_JSON_SERDE =
            new JsonSerde<>(DiscountMilestoneEmail.AggregatedStream.class);

    private static final Logger log = LoggerFactory.getLogger(DiscountMilestoneEmail.class);

    public static final int MILESTONE = 2;

    public static void main(final String[] args) {
        final StreamsBuilder builder = new StreamsBuilder();

        // configure the processing topology
        configureTopology(builder);

        // fire up the engines
        startStreams(builder);
    }

    static void configureTopology(final StreamsBuilder builder) {
        //ktable for events grouped by artist id
//        KTable<String, Event> eventByArtist = builder
//                .stream(TOPIC_DATA_DEMO_EVENTS, Consumed.with(Serdes.String(), Streams.SERDE_EVENT_JSON))
//                .selectKey((key, event) -> event.artistid())  // Re-key to artist id
//                .toTable(Materialized.<String, Event>as(persistentKeyValueStore("event-by-artist"))
//                        .withKeySerde(Serdes.String())
//                        .withValueSerde(Streams.SERDE_EVENT_JSON));
//        eventByArtist
//                .toStream()
//                .peek((key, event) -> log.info("KTable update - key: {}, event: {}", key, event))
//                .to(EVENT_OUTPUT_TOPIC, Produced.with(Serdes.String(), SERDE_EVENT_JSON));

        KStream<String, Event> eventStream = builder
                .stream(TOPIC_DATA_DEMO_EVENTS, Consumed.with(Serdes.String(), Streams.SERDE_EVENT_JSON))
                // Filter out past events by comparing the date (first 10 characters of eventdate)
                .filter((key, event) -> {
                    LocalDate eventDate = LocalDate.parse(event.eventdate().substring(0, 10));
                    return !eventDate.isBefore(LocalDate.now());
                })
                // Re-key by artist id so that we can group events per artist.
                .selectKey((key, event) -> event.artistid());

        KGroupedStream<String, Event> groupedEvents = eventStream.groupByKey(
                Grouped.with(Serdes.String(), Streams.SERDE_EVENT_JSON)
        );

        // Reduce to keep only the earliest upcoming event per artist.
        KTable<String, Event> upcomingEventByArtist = groupedEvents.reduce((event1, event2) -> {
            LocalDate d1 = LocalDate.parse(event1.eventdate().substring(0, 10));
            LocalDate d2 = LocalDate.parse(event2.eventdate().substring(0, 10));
            return d1.isBefore(d2) ? event1 : event2;
        });
        // Log updates from the upcoming event table.
        upcomingEventByArtist.toStream()
                .peek((artistId, event) -> log.info("Upcoming Event for artist {}: {}", artistId, event))
                .to(EVENT_OUTPUT_TOPIC, Produced.with(Serdes.String(), SERDE_EVENT_JSON));


        //  group the Streams by (customerId--artistId) => sum of streamtime
        KGroupedStream<String, Stream> groupedStreams = builder
                .stream(TOPIC_DATA_DEMO_STREAMS, Consumed.with(Serdes.String(), Streams.SERDE_STREAM_JSON))
                .peek((key, stream) -> log.info("received stream with key {}: {}, {}, {}",
                        key, stream.customerid(), stream.artistid(), stream.streamtime()))
                .groupBy((key, stream) -> stream.customerid() + "--" + stream.artistid(),
                        Grouped.with(Serdes.String(), Streams.SERDE_STREAM_JSON));

        // aggregate the value to count stream time count
//        KTable<String, Double> aggregatedStreamTime = groupedStreams.aggregate(
//                //initialise value to 0.0
//                () -> 0.0,
//                // add value from ktable and stream
//                (compositeKey, newStream, aggValue) ->
//                        aggValue + Double.parseDouble(newStream.streamtime()),
//                Materialized.with(Serdes.String(), Serdes.Double())
//        );

        KTable<String, Long> aggregatedStreamCount = groupedStreams.aggregate(
                // Initialize count to 0
                () -> 0L,
                // For each incoming record, increment the count by 1
                (compositeKey, newStream, currentCount) -> currentCount + 1,
                Materialized.with(Serdes.String(), Serdes.Long())
        );

        // Convert aggregated table KStream keyed by artistId, filter for every 10th streamtime count
        KStream<String, AggregatedStream> aggregatedStreamByArtist = aggregatedStreamCount.toStream()
                .map((compositeKey, totalCount) -> {
                    String[] parts = compositeKey.split("--");
                    String customerId = parts[0];
                    String artistId = parts[1];
                    AggregatedStream agg = AggregatedStream.builder()
                            .customerId(customerId)
                            .artistId(artistId)
                            .totalStreamCount(totalCount)
                            .build();
                    return new KeyValue<>(artistId, agg);
                })
                .peek((aggKey, aggStream) -> log.info("Aggregated Stream with key {}: {}, {}, {}",
                        aggKey, aggStream.customerId, aggStream.artistId, aggStream.totalStreamCount))
                .filter((artistId, agg) -> agg.getTotalStreamCount() % MILESTONE == 0)
                .peek((key, filteredStream) -> log.info("Filtered stream with key {}: {}, {}, {}",
                        key, filteredStream.customerId, filteredStream.artistId, filteredStream.totalStreamCount));

//        KStream<String, AggregatedStream> dedupAggregatedStream = aggregatedStreamByArtist
//                // Re-key with composite key (artistId--customerId)
//                .selectKey((key, agg) -> agg.getArtistId() + "--" + agg.getCustomerId())
//                // Group by the new composite key and reduce to keep the latest record
//                .groupByKey(Grouped.with(Serdes.String(), AGGREGATED_STREAM_JSON_SERDE))
//                .reduce((existing, updated) -> updated)
//                // Convert back to stream
//                .toStream()
//                // Re-key by artistId (to match the key in eventByArtist)
//                .selectKey((compositeKey, agg) -> agg.getArtistId())
//                .peek((key, dedupStream)->log.info("deduplicated stream with key {}: {}, {}, {}",
//                        key, dedupStream.customerId, dedupStream.artistId, dedupStream.totalStreamCount));

        // Join with the event KTable keyed by artistId
        KStream<String, AggregatedStreamEnriched> joinedStream = aggregatedStreamByArtist.join(
                upcomingEventByArtist,
                (agg, event) -> AggregatedStreamEnriched.builder()
                        .customerId(agg.customerId)
                        .artistId(agg.artistId)
                        .totalStreamCount(agg.totalStreamCount)
                        .venueId(event.venueid())
                        .capacity(event.capacity())
                        .eventDate(event.eventdate())
                        .build(),
                Joined.with(Serdes.String(), AGGREGATED_STREAM_JSON_SERDE, SERDE_EVENT_JSON)
        );

        // write to a new output topic
        joinedStream
                .peek((artistId, enriched) -> log.info(
                        "Joined stream => artistId={}, customerId={}, totalStreamCount={}, venueId={}, capacity={}, data={}",
                        enriched.artistId, enriched.customerId, enriched.totalStreamCount,
                        enriched.venueId, enriched.capacity, enriched.eventDate
                ))
                .to(OUTPUT_TOPIC,
                        Produced.with(Serdes.String(), AGGREGATED_STREAM_ENRICHED_JSON_SERDE));

    }

    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    public static class AggregatedStreamEnriched {
        private String customerId;
        private String artistId;
        private long totalStreamCount;
        private String venueId;
        private Integer capacity;
        private String eventDate;
    }

    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    public static class AggregatedStream {
        private String customerId;
        private String artistId;
        private long totalStreamCount;
    }

    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    public static class StreamClass {
        private String streamId;
        private String customerId;
        private String artistId;
        private String streamTime;
    }

    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    public static class DiscountMilestoneEmailConfirmation {
        // class copied from other file, needs to be changed to the final one while creating
        private String confirmationStatus;
        private String confirmationId;
        private Ticket ticketRequest;
        private Event event;
        private double remainingTickets;
    }
}


