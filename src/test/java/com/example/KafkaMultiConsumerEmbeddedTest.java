package com.example;

import com.common.Bar;
import com.common.Foo;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.SpyBean;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.annotation.DirtiesContext;

import java.util.concurrent.TimeUnit;

import static com.example.Application.MULTI_CONSUMER_TOPIC;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

@DirtiesContext
@EmbeddedKafka(partitions = 1, controlledShutdown = false, topics = MULTI_CONSUMER_TOPIC)
//@TestInstance(TestInstance.Lifecycle.PER_METHOD)
@SpringBootTest({"spring.kafka.bootstrap-servers=${spring.embedded.kafka.brokers}", "spring.kafka.consumer.auto-offset-reset=earliest"})
class KafkaMultiConsumerEmbeddedTest {

    @Autowired
    KafkaTemplate<Object, Object> template;

    @SpyBean
    @Autowired
    KafkaMultiConsumer consumer;

    @SpyBean
    MyService myService;

    @Test
    void foo() throws InterruptedException {
        template.send(MULTI_CONSUMER_TOPIC, new Foo("foo"));

        consumer.getLatch().await(5, TimeUnit.SECONDS);

        verify(consumer, atLeast(1)).foo(any(Foo.class), any(Acknowledgment.class));
    }

    @Test
    void bar() throws InterruptedException {
        template.send(MULTI_CONSUMER_TOPIC, new Bar("bar"));

        consumer.getLatch().await(5, TimeUnit.SECONDS);

        verify(consumer, atLeast(1)).bar(any(Bar.class), any(Acknowledgment.class));
        verify(myService, times(1)).doSomething(any(Bar.class));
    }

    @Test
    void unknown() throws InterruptedException {
        template.send(MULTI_CONSUMER_TOPIC, "unknown");

        consumer.getLatch().await(5, TimeUnit.SECONDS);

        verify(consumer, atLeast(1)).unknown(any(), any(Acknowledgment.class));
    }

}