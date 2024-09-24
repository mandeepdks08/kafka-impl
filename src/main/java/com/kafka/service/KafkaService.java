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
		if (!topics.containsKey(topic)) {
			throw new RuntimeException("Topic does not exist");
		}
		topics.get(topic).push(data, key);
	}
}
