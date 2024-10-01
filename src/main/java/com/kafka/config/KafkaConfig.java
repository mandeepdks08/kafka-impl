package com.kafka.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.kafka.service.KafkaService;

@Configuration
public class KafkaConfig {
	
	@Bean
	public KafkaService kafkaService() {
		return new KafkaService();
	}
}
