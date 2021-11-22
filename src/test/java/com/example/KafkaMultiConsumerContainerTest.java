package com.example;

import com.common.Bar;
import com.common.Foo;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.test.mock.mockito.SpyBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.test.annotation.DirtiesContext;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;
import java.util.Map;

import static com.example.Application.MULTI_CONSUMER_TOPIC;
import static org.assertj.core.api.BDDAssertions.then;
import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

@SpringBootTest( {"spring.kafka.bootstrap-servers=localhost:29092", "spring.kafka.consumer.auto-offset-reset=latest"})
@DirtiesContext
@Testcontainers
@Import(KafkaMultiConsumerContainerTest.KafkaTestContainersConfiguration.class)
public class KafkaMultiConsumerContainerTest {

    @SuppressWarnings("SpellCheckingInspection")
    @Container
    private static final KafkaContainer kafka = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:latest"));

    @TestConfiguration
    static class KafkaTestContainersConfiguration {
        @Bean
        public KafkaAdmin kafkaAdmin() {
            return new KafkaAdmin(Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers()));
        }
    }

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

        consumer.getLatch().await();

        verify(consumer, atLeast(1)).foo(any(Foo.class), any(Acknowledgment.class));
    }

    @Test
    void bar() throws InterruptedException {
        template.send(MULTI_CONSUMER_TOPIC, new Bar("bar"));

        consumer.getLatch().await();

        verify(consumer, atLeast(1)).bar(any(Bar.class), any(Acknowledgment.class));
        verify(myService, times(1)).doSomething(any(Bar.class));
    }

    @Test
    void unknown() throws InterruptedException {
        template.send(MULTI_CONSUMER_TOPIC, "unknown");

        consumer.getLatch().await();

        verify(consumer, atLeast(1)).unknown(any(),any(Acknowledgment.class));
    }
}
