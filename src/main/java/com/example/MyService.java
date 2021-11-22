package com.example;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

@Slf4j
@Service
public class MyService {

    @Getter
    private Object receivedPayload;

    public void exceptionThrown() {
        throw new RuntimeException("thrown at save");
    }

    public void doSomething(Object obj) {
        log.info("doSomething, obj=[{}]\n", obj);
        receivedPayload = obj;
    }
}
