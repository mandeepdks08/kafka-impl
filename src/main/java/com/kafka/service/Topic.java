package com.kafka.service;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import lombok.Getter;

@Getter
public class Topic {
	private String name;
	private List<Partition> partitions;
	private Integer ttlInSeconds;
	private final int[] allowedPartitionsSize = { 1, 2, 4, 8, 16, 32 };
	private final int DEFAULT_TTL_SECONDS = 30 * 60; // 30 minutes

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
	}
}
