package com.kafka.service;

import java.util.ArrayList;
import java.util.List;

import lombok.Getter;

public class Consumer {
	@Getter
	private final String consumerId;
	private List<Integer> assignedPartitionIndices;
	private Integer lastPollPartitionIndex;

	public Consumer(String consumerId) {
		this.consumerId = consumerId;
		this.assignedPartitionIndices = new ArrayList<>();
	}

	public void addPartitionIndex(int newPartitionIndex) {
		assignedPartitionIndices.add(newPartitionIndex);
		assignedPartitionIndices.sort((index1, index2) -> index1.compareTo(index2));
	}

	public int getTotalPartitionsAssigned() {
		return assignedPartitionIndices.size();
	}

	public int getNextPollPartitionIndex() {
		if (lastPollPartitionIndex == null) {
			lastPollPartitionIndex = 0;
			return lastPollPartitionIndex;
		}
		lastPollPartitionIndex = (lastPollPartitionIndex + 1) % assignedPartitionIndices.size();
		return assignedPartitionIndices.get(lastPollPartitionIndex);
	}

	public void reset() {
		assignedPartitionIndices.clear();
		lastPollPartitionIndex = null;
	}
}
