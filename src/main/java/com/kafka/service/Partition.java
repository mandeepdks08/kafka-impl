package com.kafka.service;

import com.kafka.datamodel.Record;

import lombok.Getter;

public class Partition {
	private Record head;
	private Record tail;
	private Record next;
	@Getter
	private final int partitionIndex;

	public Partition(int partitionIndex) {
		this.partitionIndex = partitionIndex;
	}

	public void push(String data, Integer ttlInSeconds) {
		Record newRecord = new Record(data, ttlInSeconds);
		synchronized (this) {
			if (isPartitionEmpty()) {
				head = newRecord;
				tail = newRecord;
				next = head;
			} else {
				tail.setNext(newRecord);
				tail = newRecord;
			}
		}
	}

	public String poll() {
		synchronized (this) {
			if (isPartitionEmpty()) {
				return null;
			}
			if (next.isExpired()) {
				next = head;
			}
			String data = next.getData();
			next = next.getNext();
			return data;
		}
	}

	public void deleteExpiredRecords() {
		synchronized (this) {
			while (head != null && head.isExpired()) {
				Record previousRecord = head;
				head = head.getNext();
				previousRecord.setNext(null);
			}
		}
	}

	private boolean isPartitionEmpty() {
		synchronized (this) {
			return head == null;
		}
	}
}
