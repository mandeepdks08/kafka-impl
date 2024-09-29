package com.kafka.service;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.lang3.StringUtils;

import lombok.Getter;

public class Topic {
	@Getter
	private String name;
	private List<Partition> partitions;
	private List<Consumer> consumers;
	private Integer ttlInSeconds;
	private Integer lastPartitionPushedInto;
	private static final int[] allowedPartitionsSize = { 1, 2, 4, 8, 16, 32 };
	private static final int DEFAULT_TTL_SECONDS = 30 * 60; // 30 minutes

	public Topic(final String name, final Integer partitions, final Integer ttlInSeconds) {
		this.name = name;
		initPartitions(partitions);
		this.ttlInSeconds = (ttlInSeconds == null || ttlInSeconds <= 0) ? DEFAULT_TTL_SECONDS : ttlInSeconds;
		this.lastPartitionPushedInto = 0;
		this.consumers = new ArrayList<>();
	}

	private void initPartitions(final Integer partitions) {
		this.partitions = new ArrayList<>(1);
		if (partitions != null && Arrays.stream(allowedPartitionsSize).anyMatch(size -> size == partitions)) {
			this.partitions = new ArrayList<>(partitions);
		}
		for (int i = 0; i < partitions; i++) {
			this.partitions.set(i, new Partition(i, this.name));
		}
	}

	public void push(String data, String key) {
		Partition partition = getPartitionToPushDataInto(key);
		partition.push(data, ttlInSeconds);
	}

	public void registerConsumer(String consumerId) {
		if (consumers.stream().anyMatch(consumer -> consumer.getConsumerId().equals(consumerId))) {
			throw new RuntimeException("Consumer with consumerId " + consumerId + " already registered");
		} else if (consumers.size() == partitions.size()) {
			throw new RuntimeException("Consumers limit reached for topic " + name);
		}
		int partitionsLeftToAssign = partitions.size();
		consumers.add(new Consumer(consumerId));
		consumers.stream().forEach(Consumer::reset);
		int consumerIndex = 0;
		while (partitionsLeftToAssign > 0) {
			consumers.get(consumerIndex).addPartitionIndex(partitionsLeftToAssign - 1);
			consumerIndex = (consumerIndex + 1) % consumers.size();
			partitionsLeftToAssign--;
		}
	}

	public String poll(String consumerId) {
		Consumer consumer = consumers.stream().filter(cs -> cs.getConsumerId().equals(consumerId)).findFirst()
				.orElse(null);
		if (consumer == null) {
			throw new RuntimeException("Consumer not registered");
		}
		int totalPartitionsAssigned = consumer.getTotalPartitionsAssigned();
		int totalPartitionsChecked = 0;
		while (totalPartitionsChecked < totalPartitionsAssigned) {
			final int nextPollPartitionIndex = consumer.getNextPollPartitionIndex();
			Partition partition = partitions.stream().filter(p -> p.getPartitionIndex() == nextPollPartitionIndex)
					.findFirst().get();
			totalPartitionsChecked++;
			String data = partition.poll();
			if (data != null) {
				return data;
			}
		}
		return null;
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
