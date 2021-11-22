package com.example;

import com.common.Bar;
import com.common.Foo;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaHandler;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

import java.util.concurrent.CountDownLatch;


@Slf4j
@Component
@KafkaListener(id = "multi-group", topics = Application.MULTI_CONSUMER_TOPIC)
@RequiredArgsConstructor
public class KafkaMultiConsumer {

    @Getter
    private CountDownLatch latch = new CountDownLatch(1);

    private final MyService myService;

    @KafkaHandler
    public void foo(@Payload Foo foo, Acknowledgment acknowledgment) {
        log.info("Received Foo:[{}]", foo);

        try {
            myService.exceptionThrown();
        } catch (Exception e) {
            log.error("error in foo, errorMessage=[{}]", e.getMessage());
            throw new RuntimeException();
        }

        latch.countDown();
        acknowledgment.acknowledge();
    }

    @KafkaHandler
    public void bar(@Payload Bar bar, Acknowledgment acknowledgment) {
        log.info("Received Bar:[{}]", bar);

        try {
            myService.exceptionThrown();
        } finally {
            latch.countDown();
        }
        acknowledgment.acknowledge();
    }

    @KafkaHandler(isDefault = true)
    public void unknown(@Payload Object object, Acknowledgment acknowledgment) {
        log.info("Received unknown:[{}]", object);
        latch.countDown();

        acknowledgment.acknowledge();
    }

}
