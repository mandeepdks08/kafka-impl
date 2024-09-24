package com.kafka.service;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;

import lombok.Getter;

public class Topic {
	@Getter
	private String name;
	private List<Partition> partitions;
	private Map<String, List<Partition>> consumersPartitionMap;
	private Integer ttlInSeconds;
	private Integer lastPartitionPushedInto;
	private static final int[] allowedPartitionsSize = { 1, 2, 4, 8, 16, 32 };
	private static final int DEFAULT_TTL_SECONDS = 30 * 60; // 30 minutes

	public Topic(final String name, final Integer partitions, final Integer ttlInSeconds) {
		this.name = name;
		this.partitions = new ArrayList<>(1);
		if (partitions != null && Arrays.stream(allowedPartitionsSize).anyMatch(size -> size == partitions)) {
			this.partitions = new ArrayList<>(partitions);
		}
		for (int i = 0; i < partitions; i++) {
			this.partitions.set(i, new Partition());
		}
		this.ttlInSeconds = (ttlInSeconds == null || ttlInSeconds <= 0) ? DEFAULT_TTL_SECONDS : ttlInSeconds;
		this.lastPartitionPushedInto = 0;
		this.consumersPartitionMap = new HashMap<>();
	}

	public void push(String data, String key) {
		Partition partition = getPartitionToPushDataInto(key);
		partition.push(data, ttlInSeconds);
	}

	public void registerConsumer(String consumerId) {
		if (consumersPartitionMap.containsKey(consumerId)) {
			throw new RuntimeException("Consumer with consumerId " + consumerId + " already registered");
		} else if (consumersPartitionMap.size() == partitions.size()) {
			throw new RuntimeException("Consumers limit reached for topic " + name);
		}
		int partitionsLeftToAssign = partitions.size();
		List<String> consumerIds = new ArrayList<>(consumersPartitionMap.keySet());
		consumerIds.add(consumerId);
		int consumerIdIndex = 0;
		consumersPartitionMap.clear();
		while (partitionsLeftToAssign > 0) {
			String consId = consumerIds.get(consumerIdIndex);
			if (!consumersPartitionMap.containsKey(consId)) {
				consumersPartitionMap.put(consId, new ArrayList<>());
			}
			consumersPartitionMap.get(consId).add(partitions.get(partitionsLeftToAssign - 1));
			partitionsLeftToAssign--;
			consumerIdIndex = (consumerIdIndex + 1) % consumerIds.size();
		}
	}

	private Partition getPartitionToPushDataInto(String key) {
		int partitionsSize = partitions.size();
		Integer partitionIndex = null;
		if (StringUtils.isBlank(key)) {
			partitionIndex = (lastPartitionPushedInto + 1) % partitionsSize;
		} else {
			partitionIndex = key.hashCode() % partitionsSize;
		}
		lastPartitionPushedInto = partitionIndex;
		return partitions.get(partitionIndex);
	}

}
