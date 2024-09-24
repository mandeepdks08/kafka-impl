package com.kafka.service;

import org.apache.commons.lang3.ObjectUtils;

import com.kafka.datamodel.Record;

public class Partition {
	private Record head;
	private Record tail;
	private Record next;

	public void push(String data, Integer ttlInSeconds) {
		Record newRecord = new Record(data, ttlInSeconds);
		if (isPartitionEmpty()) {
			head = newRecord;
			tail = newRecord;
			next = head;
		} else {
			tail.setNext(newRecord);
			tail = newRecord;
		}
	}

	public String poll() {
		if (isPartitionEmpty()) {
			return null;
		}
		if (next.isExpired()) {
			next = head;
		}
		return next.getData();
	}

	public void acknowledge() {
		if (next == null) {
			next = head;
		} else {
			next = ObjectUtils.firstNonNull(next.getNext(), head);
		}
	}
	
	public void deleteExpiredRecords() {
		while (head != null && head.isExpired()) {
			Record previousRecord = head;
			head = head.getNext();
			previousRecord.setNext(null);
		}
	}

	private boolean isPartitionEmpty() {
		return head == null;
	}
}
