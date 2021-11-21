package com.example;

import com.common.Bar2;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.boot.autoconfigure.kafka.ConcurrentKafkaListenerContainerFactoryConfigurer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.dao.RecoverableDataAccessException;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.retry.backoff.BackOffPolicy;
import org.springframework.retry.backoff.ExponentialBackOffPolicy;
import org.springframework.retry.policy.SimpleRetryPolicy;
import org.springframework.retry.support.RetryTemplate;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeoutException;

@Configuration
@Slf4j
public class ConsumerConfig {

	@Bean
	ConcurrentKafkaListenerContainerFactory<?, ?> kafkaListenerContainerFactory(
			ConcurrentKafkaListenerContainerFactoryConfigurer configurer,
			ConsumerFactory<Object, Object> kafkaConsumerFactory) {
		ConcurrentKafkaListenerContainerFactory<Object, Object> factory = new ConcurrentKafkaListenerContainerFactory<>();
		configurer.configure(factory, kafkaConsumerFactory);
		factory.setConcurrency(10);
		factory.setRecoveryCallback((context -> {
			// here you can log things and throw some custom exception that Error handler will take care of ..
			log.error("normal error = [{}]", context.getLastThrowable().getClass().getCanonicalName());
			Object record = context.getAttribute("record");
			if (record instanceof ConsumerRecord) {
				ConsumerRecord consumerRecord = (ConsumerRecord) record;
				Object value = consumerRecord.value();
				if (value instanceof Bar2) {
					log.info("consumed error record is:[{}]", value);
					Bar2 bar2 = (Bar2) value;
					log.info("Bar2 bar is:[{}]", bar2.getBar());
				}
			}
			return null;
		}));

		factory.setErrorHandler(((exception, data) -> {
           /* here you can do you custom handling, I am just logging it same as default Error handler does
          If you just want to log. you need not configure the error handler here. The default handler does it for you.
          Generally, you will persist the failed records to DB for tracking the failed records.  */
			log.error("Error in process with Exception {} and the record is {}", exception, data);
		}));

//		factory.setRetryTemplate(retryTemplate());

		return factory;
	}

	private RetryTemplate retryTemplate() {

		RetryTemplate retryTemplate = new RetryTemplate();

		/* here retry policy is used to set the number of attempts to retry and what exceptions you wanted to try and what you don't want to retry.*/

		retryTemplate.setRetryPolicy(getSimpleRetryPolicy());
//		retryTemplate.setBackOffPolicy(getBackOffPolicy());
		return retryTemplate;
	}

	private BackOffPolicy getBackOffPolicy() {
		ExponentialBackOffPolicy exponentialBackOffPolicy = new ExponentialBackOffPolicy();
		exponentialBackOffPolicy.setInitialInterval(ExponentialBackOffPolicy.DEFAULT_INITIAL_INTERVAL);
		exponentialBackOffPolicy.setMaxInterval(Duration.ofMinutes(5).toMillis());
		exponentialBackOffPolicy.setMultiplier(5);

		return exponentialBackOffPolicy;
	}

	private SimpleRetryPolicy getSimpleRetryPolicy() {
		Map<Class<? extends Throwable>, Boolean> exceptionMap = new HashMap<>();
		exceptionMap.put(IllegalStateException.class, true);
		exceptionMap.put(TimeoutException.class, true);
		SimpleRetryPolicy simpleRetryPolicy = new SimpleRetryPolicy(5, exceptionMap, true);

		return simpleRetryPolicy;
	}

}
