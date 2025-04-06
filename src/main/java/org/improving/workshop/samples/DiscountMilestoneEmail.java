package org.improving.workshop.samples;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.improving.workshop.Streams;
import org.msse.demo.mockdata.customer.email.Email;
import org.msse.demo.mockdata.customer.profile.Customer;
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

    public static final JsonSerde<DiscountMilestoneEmail.MinimalCustomer> MINIMAL_CUSTOMER_JSON_SERDE =
            new JsonSerde<>(DiscountMilestoneEmail.MinimalCustomer.class);

    public static final JsonSerde<DiscountMilestoneEmail.EnrichedCustomer> ENRICHED_CUSTOMER_JSON_SERDE =
            new JsonSerde<>(DiscountMilestoneEmail.EnrichedCustomer.class);

    public static final JsonSerde<DiscountMilestoneEmail.FinalEnrichedOutput> FINAL_ENRICHED_JSON_SERDE =
            new JsonSerde<>(DiscountMilestoneEmail.FinalEnrichedOutput.class);

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

        KTable<String, MinimalCustomer> minimalCustomerTable = builder
                .stream(TOPIC_DATA_DEMO_CUSTOMERS, Consumed.with(Serdes.String(), SERDE_CUSTOMER_JSON))
                .mapValues(customer ->
                        MinimalCustomer.builder()
                                .customerId(customer.id())
                                .fName(customer.fname())
                                .lName(customer.lname())
                                .title(customer.title())
                                .gender(customer.gender())
                                .build()
                ).peek((key, customer) -> log.info("Received customer record: key={}, value={}", key, customer))
                .toTable(Materialized.<String, MinimalCustomer>as(persistentKeyValueStore("minimal-customer-store"))
                        .withKeySerde(Serdes.String())
                        .withValueSerde(MINIMAL_CUSTOMER_JSON_SERDE));


        KTable<String, Email> emailTable = builder
                .stream(TOPIC_DATA_DEMO_EMAILS, Consumed.with(Serdes.String(), SERDE_EMAIL_JSON))
                // Re-key by customerid
                .selectKey((oldKey, email) -> email.customerid())
                .peek((key, email) -> log.info("Received Email record: key={}, value={}", key, email))
                .toTable(Materialized.<String, Email>as(persistentKeyValueStore("email-store"))
                        .withKeySerde(Serdes.String())
                        .withValueSerde(SERDE_EMAIL_JSON));

        KTable<String, EnrichedCustomer> enrichedCustomerTable = minimalCustomerTable.join(
                emailTable,
                (minimalCustomer, email) -> EnrichedCustomer.builder()
                        .customerId(minimalCustomer.getCustomerId())
                        .fName(minimalCustomer.fName)
                        .lName(minimalCustomer.lName)
                        .title(minimalCustomer.title)
                        .gender(minimalCustomer.gender)
                        .email(email.email())
                        .build(),
                Materialized.<String, EnrichedCustomer>as(persistentKeyValueStore("enriched-customer-store"))
                        .withKeySerde(Serdes.String())
                        .withValueSerde(ENRICHED_CUSTOMER_JSON_SERDE)
        );

        KStream<String, EnrichedCustomer> enrichedCustomerStream = enrichedCustomerTable.toStream()
                .peek((key, value) -> log.info("Enriched Customer record: key={}, value={}", key, value));


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
                        .eventId(event.id())
                        .venueId(event.venueid())
                        .capacity(event.capacity())
                        .eventDate(event.eventdate())
                        .build(),
                Joined.with(Serdes.String(), AGGREGATED_STREAM_JSON_SERDE, SERDE_EVENT_JSON)
        );

        // write to a new output topic
//        joinedStream
//                .peek((artistId, enriched) -> log.info(
//                        "Joined stream => artistId={}, customerId={}, totalStreamCount={}, eventId={}, venueId={}, capacity={}, date={}",
//                        enriched.artistId, enriched.customerId, enriched.totalStreamCount,
//                        enriched.eventId, enriched.venueId, enriched.capacity, enriched.eventDate
//                ))
//                .to(OUTPUT_TOPIC,
//                        Produced.with(Serdes.String(), AGGREGATED_STREAM_ENRICHED_JSON_SERDE));


        KStream<String, AggregatedStreamEnriched> rekeyedJoinedStream = joinedStream
                .selectKey((oldKey, value) -> value.getCustomerId());

        KStream<String, FinalEnrichedOutput> finalEnrichedStream = rekeyedJoinedStream.join(
                enrichedCustomerTable,
                (aggEnriched, enrichedCustomer) -> FinalEnrichedOutput.builder()
                        .customerId(aggEnriched.getCustomerId())
                        .artistId(aggEnriched.getArtistId())
                        .totalStreamCount(aggEnriched.getTotalStreamCount())
                        .eventId(aggEnriched.getEventId())
                        .venueId(aggEnriched.getVenueId())
                        .capacity(aggEnriched.getCapacity())
                        .eventDate(aggEnriched.getEventDate())
                        .fName(enrichedCustomer.getFName())
                        .lName(enrichedCustomer.getLName())
                        .title(enrichedCustomer.getTitle())
                        .gender(enrichedCustomer.getGender())
                        .email(enrichedCustomer.getEmail())
                        .build(),
                Joined.with(
                        Serdes.String(),
                        AGGREGATED_STREAM_ENRICHED_JSON_SERDE,
                        ENRICHED_CUSTOMER_JSON_SERDE
                ));

        finalEnrichedStream
                .peek((key, value) -> log.info("Final Enriched Output: key={}, value={}", key, value))
                .to(OUTPUT_TOPIC, Produced.with(Serdes.String(), FINAL_ENRICHED_JSON_SERDE));

    }

    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    public static class FinalEnrichedOutput {
        private String customerId;
        private String artistId;
        private long totalStreamCount;
        private String eventId;
        private String venueId;
        private Integer capacity;
        private String eventDate;
        // Fields from EnrichedCustomer
        private String fName;
        private String lName;
        private String title;
        private String gender;
        private String email;
    }


    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    public static class EnrichedCustomer {
        private String customerId;
        private String fName;
        private String lName;
        private String title;
        private String gender;
        private String email;
    }


    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    public static class MinimalCustomer {
        private String customerId;
        private String fName;
        private String lName;
        private String title;
        private String gender;
    }


    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    public static class AggregatedStreamEnriched {
        private String customerId;
        private String artistId;
        private long totalStreamCount;
        private String eventId;
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


