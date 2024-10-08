package com.kafka.service;

import java.util.ArrayList;
import java.util.List;

import com.kafka.datamodel.Record;

public class Batch {
	private final int startOffset;
	private final int capacity;
	private List<Record> records;

	public Batch(int startOffset, int capacity) {
		this.startOffset = startOffset;
		this.capacity = capacity;
		this.records = new ArrayList<>(capacity);
	}

	public void append(Record record) {
		if (isFull()) {
			throw new RuntimeException("Batch full");
		}
		records.add(record);
	}

	public int getStartOffset() {
		return startOffset;
	}

	public int getCapacity() {
		return capacity;
	}

	public boolean isFull() {
		return records.size() == capacity;
	}

	public int size() {
		return records.size();
	}

	public boolean isEmpty() {
		return records.size() == 0;
	}

	public Record getRecord(int index) {
		if (index < records.size()) {
			return records.get(index);
		} else {
			return null;
		}
	}

	public boolean isExpired() {
		return !isEmpty() && records.get(records.size() - 1).isExpired();
	}

}
