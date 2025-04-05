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
import org.msse.demo.mockdata.music.ticket.Ticket;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.support.serializer.JsonSerde;

import java.util.UUID;

import static org.apache.kafka.streams.state.Stores.persistentKeyValueStore;
import static org.improving.workshop.Streams.*;

public class DiscountMilestoneEmail {

    public static final String OUTPUT_TOPIC = "kafka-workshop-discount-milestone-email-response";

    public static final JsonSerde<DiscountMilestoneEmail.DiscountMilestoneEmailConfirmation> DISCOUNT_MILESTONE_EVENT_JSON_SERDE
        = new JsonSerde<>(DiscountMilestoneEmailConfirmation.class);

    // stream class for test
    public static final JsonSerde<DiscountMilestoneEmail.StreamClass> STREAM_CLASS_JSON_SERDE
            = new JsonSerde<>(StreamClass.class);

    private static final Logger log = LoggerFactory.getLogger(DiscountMilestoneEmail.class);

    public static final int MILESTONE = 3;

    public static void main(final String[] args) {
        final StreamsBuilder builder = new StreamsBuilder();

        // configure the processing topology
        configureTopology(builder);

        // fire up the engines
        startStreams(builder);
    }

    static void configureTopology(final StreamsBuilder builder) {

        KStream<String, Stream> streamRecords = builder
                .stream(TOPIC_DATA_DEMO_STREAMS, Consumed.with(Serdes.String(), Streams.SERDE_STREAM_JSON))
                .peek((key, stream) -> log.info
                        ("Received stream record: streamId= {}, customerId={}, artistId={}, streamTime={}",
                        stream.id(), stream.customerid(), stream.artistid(), stream.streamtime()));

        KStream<String, DiscountMilestoneEmail.StreamClass> convertedStream = streamRecords
                .mapValues(stream -> DiscountMilestoneEmail.StreamClass.builder()
                        .streamId(UUID.randomUUID().toString())
                        .customerId(stream.customerid())
                        .artistId(stream.artistid())
                        .streamTime(stream)
                        .build());

        convertedStream.to(OUTPUT_TOPIC, Produced.with(Serdes.String(), STREAM_CLASS_JSON_SERDE));

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

    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    public static class StreamClass {
        private String streamId;
        private String customerId;
        private String artistId;
        private Stream streamTime;
    }
}


