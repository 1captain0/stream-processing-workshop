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

import java.util.UUID;

import static org.apache.kafka.streams.state.Stores.persistentKeyValueStore;
import static org.improving.workshop.Streams.*;

public class DiscountMilestoneEmail {

    public static final String OUTPUT_TOPIC = "kafka-workshop-discount-milestone-email";

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

    public static final int MILESTONE = 10;

    public static void main(final String[] args) {
        final StreamsBuilder builder = new StreamsBuilder();

        // configure the processing topology
        configureTopology(builder);

        // fire up the engines
        startStreams(builder);
    }

    static void configureTopology(final StreamsBuilder builder) {
        //ktable for events grouped by artist id
        KTable<String, Event> eventByArtist = builder
                .stream(TOPIC_DATA_DEMO_EVENTS, Consumed.with(Serdes.String(), Streams.SERDE_EVENT_JSON))
                .selectKey((key, event) -> event.artistid())  // Re-key to artist id
                .toTable(Materialized.<String, Event>as(persistentKeyValueStore("event-by-artist"))
                        .withKeySerde(Serdes.String())
                        .withValueSerde(Streams.SERDE_EVENT_JSON));
        eventByArtist
                .toStream()
                .peek((key, event) -> log.info("KTable update - key: {}, event: {}", key, event));

        //  group the Streams by (customerId--artistId) => sum of streamtime
        KGroupedStream<String, Stream> groupedStreams = builder
                .stream(TOPIC_DATA_DEMO_STREAMS, Consumed.with(Serdes.String(), Streams.SERDE_STREAM_JSON))
                .peek((key, stream) -> log.info("received stream with key {}: {}, {}, {}",
                        key, stream.customerid(), stream.artistid(), stream.streamtime()))
                .groupBy((key, stream) -> stream.customerid() + "--" + stream.artistid(),
                        Grouped.with(Serdes.String(), Streams.SERDE_STREAM_JSON));

        // aggregate the value to count stream time count
        KTable<String, Double> aggregatedStreamTime = groupedStreams.aggregate(
                //initialise value to 0.0
                () -> 0.0,
                // add value from ktable and stream
                (compositeKey, newStream, aggValue) ->
                        aggValue + Double.parseDouble(newStream.streamtime()),
                Materialized.with(Serdes.String(), Serdes.Double())
        );

        // Convert aggregated table KStream keyed by artistId, filter for every 10th streamtime count
        KStream<String, AggregatedStream> aggregatedStreamByArtist = aggregatedStreamTime.toStream()
                .map((compositeKey, totalTime) -> {
                    String[] parts = compositeKey.split("--");
                    String customerId = parts[0];
                    String artistId = parts[1];
                    AggregatedStream agg = AggregatedStream.builder()
                            .customerId(customerId)
                            .artistId(artistId)
                            .totalStreamTime(totalTime)
                            .build();
                    return new KeyValue<>(artistId, agg);
                })
                .peek((aggKey, aggStream) -> log.info("Aggregated Stream with key {}: {}, {}, {}",
                        aggKey, aggStream.customerId, aggStream.artistId, aggStream.totalStreamTime))
                .filter((artistId, agg) -> agg.getTotalStreamTime() % MILESTONE == 0)
                .peek((key, filteredStream) -> log.info("Filtered stream with key {}: {}, {}, {}",
                        key, filteredStream.customerId, filteredStream.artistId, filteredStream.totalStreamTime));

        // Join with the event KTable keyed by artistId
        KStream<String, AggregatedStreamEnriched> joinedStream = aggregatedStreamByArtist.join(
                eventByArtist,
                (agg, event) -> AggregatedStreamEnriched.builder()
                        .customerId(agg.customerId)
                        .artistId(agg.artistId)
                        .totalStreamTime(agg.totalStreamTime)
                        .venueId(event.venueid())
                        .capacity(event.capacity())
                        .eventDate(event.eventdate())
                        .build(),
                Joined.with(Serdes.String(), AGGREGATED_STREAM_JSON_SERDE, SERDE_EVENT_JSON)
        ).peek((key,enrichedStream) -> log.info(
                "Joined stream => artistId={}, customerId={}, totalStreamTime={}, venueId={}, capacity={}, data={}",
                enrichedStream.artistId, enrichedStream.customerId, enrichedStream.totalStreamTime,
                enrichedStream.venueId, enrichedStream.capacity, enrichedStream.eventDate));

        // write to a new output topic
        joinedStream
                .peek((artistId, enriched) -> log.info(
                        "Joined stream => artistId={}, customerId={}, totalStreamTime={}, venueId={}, capacity={}, data={}",
                        enriched.artistId, enriched.customerId, enriched.totalStreamTime,
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
        private double totalStreamTime;
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
        private double totalStreamTime;
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


