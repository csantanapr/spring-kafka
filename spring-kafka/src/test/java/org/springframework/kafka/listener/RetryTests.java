/*
 * Copyright 2020 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.springframework.kafka.listener;

/**
 * @author Gary Russell
 * @since 5.2
 *
 */
import static org.assertj.core.api.Assertions.assertThat;

import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.junit.jupiter.api.Test;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.KafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerializer;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;
import org.springframework.util.backoff.FixedBackOff;

@SpringJUnitConfig
@EmbeddedKafka(topics = "sr2", controlledShutdown = true, partitions = 1, count = 1)
@DirtiesContext
public class RetryTests {

	private static final String DEFAULT_TEST_GROUP_ID = "statefulRetry";

	private static int MAX_RETRY = 2;

	@Autowired
	private Config config;

	@Autowired
	private KafkaTemplate<Integer, String> template;

	@Test
	public void testStatefulRetry() throws Exception {
		this.template.send("sr2", "foo");
		Thread.sleep(10000);
		assertThat(this.config.listener1().count).isEqualTo(MAX_RETRY + 1);
	}

	@Configuration
	@EnableKafka
	public static class Config {
		@Autowired
		private EmbeddedKafkaBroker embeddedKafkaBroker;

		@Bean
		public KafkaListenerContainerFactory<?> kafkaListenerContainerFactory() {
			ConcurrentKafkaListenerContainerFactory<Integer, String> factory = new ConcurrentKafkaListenerContainerFactory<>();
			factory.setConsumerFactory(consumerFactory());
			factory.setBatchListener(true);
			SeekToCurrentBatchErrorHandler errorHandler = new SeekToCurrentBatchErrorHandler();
			FixedBackOff backOff = new FixedBackOff(500, MAX_RETRY);
			errorHandler.setBackOff(backOff);
			factory.setBatchErrorHandler(errorHandler);
			return factory;
		}

		@Bean
		public ConsumerFactory<Integer, String> consumerFactory() {
			return new DefaultKafkaConsumerFactory<>(consumerConfigs(200),
					new IntegerDeserializer(),
					new JsonDeserializer<>(String.class));
		}

		private Map<String, Object> consumerConfigs(int maxPollRecord) {
			Map<String, Object> consumerProps = KafkaTestUtils.consumerProps(DEFAULT_TEST_GROUP_ID, "false",
					embeddedKafkaBroker);
			consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
			consumerProps.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, maxPollRecord);
			consumerProps.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, 300000);
			return consumerProps;
		}

		@Bean
		public KafkaTemplate<Integer, String> template() {
			KafkaTemplate<Integer, String> kafkaTemplate = new KafkaTemplate<>(producerFactory());
			return kafkaTemplate;
		}

		@Bean
		public ProducerFactory<Integer, String> producerFactory() {
			return new DefaultKafkaProducerFactory<>(producerConfigs());
		}

		@Bean
		public Map<String, Object> producerConfigs() {
			Map<String, Object> config = KafkaTestUtils.producerProps(embeddedKafkaBroker);
			config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);
			return config;
		}

		@Bean
		public Listener listener1() {
			return new Listener();
		}

	}

	public static class Listener {

		public volatile int count = 0;

		@KafkaListener(topics = "sr2", groupId = "sr1")
		public void listen1(final String in, @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
				@Header(KafkaHeaders.RECEIVED_PARTITION_ID) int partition,
				@Header(KafkaHeaders.OFFSET) long offset) {
			count++;
			throw new RuntimeException("consumer throw exception");
		}

	}

}
