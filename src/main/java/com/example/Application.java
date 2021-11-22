package com.example;

import com.common.Bar;
import com.common.Foo;
import lombok.RequiredArgsConstructor;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.UUID;

import static com.example.Application.MULTI_CONSUMER_TOPIC;


@SpringBootApplication
public class Application {

    public static final String MULTI_CONSUMER_TOPIC = "multi-consumer-topic";

    public static void main(String[] args) {
        SpringApplication.run(Application.class, args);
    }

}

@RestController
@RequiredArgsConstructor
class Controller {

    private final KafkaTemplate<Object, Object> template;

    @PostMapping(path = "/send/foo/{what}")
    public void sendFoo(@PathVariable String what) {
        send(new Foo(what));
    }

    @PostMapping(path = "/send/bar/{what}")
    public void sendBar(@PathVariable String what) {
        send(new Bar(what));
    }

    @PostMapping(path = "/send/unknown/{what}")
    public void sendUnknown(@PathVariable String what) {
        send(what);
    }


    public void send(Object obj) {
        template.send(MULTI_CONSUMER_TOPIC, UUID.randomUUID().toString(), obj);
    }

}
