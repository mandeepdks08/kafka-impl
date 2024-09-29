package com.kafka.service;

import java.util.HashMap;
import java.util.Map;

public class KafkaService {
	private Map<String, Topic> topics;

	public KafkaService() {
		this.topics = new HashMap<>();
	}

	public void createTopic(String name, Integer partitions, Integer ttlInSeconds) {
		if (topics.containsKey(name)) {
			throw new RuntimeException("Topic already exist");
		}
		Topic newTopic = new Topic(name, partitions, ttlInSeconds);
		topics.put(name, newTopic);
	}

	public void push(String topic, String data) {
		push(topic, data, null);
	}

	public void push(String topic, String data, String key) {
		validateTopic(topic);
		topics.get(topic).push(data, key);
	}

	public void registerConsumer(String topic, String consumerId) {
		validateTopic(topic);
		topics.get(topic).registerConsumer(consumerId);
	}
	
	public String poll(String topic, String consumerId) {
		validateTopic(topic);
		return topics.get(topic).poll(consumerId);
	}
	
	private void validateTopic(String topic) {
		if (!topics.containsKey(topic)) {
			throw new RuntimeException("Topic does not exist");
		}
	}
}
