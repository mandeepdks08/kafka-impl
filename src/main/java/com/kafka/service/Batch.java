package com.kafka.service;

import java.util.ArrayList;
import java.util.List;

import com.kafka.datamodel.Record;
import com.kafka.util.GsonUtils;

public class Batch {
	private final int startOffset;
	private final int capacity;
	private List<Record> records;
	private int size;

	public Batch(int startOffset, int capacity) {
		this.startOffset = startOffset;
		this.capacity = capacity;
		this.records = new ArrayList<>(capacity);
		this.size = 0;
	}

	public void append(Record record) {
		if (isFull()) {
			throw new RuntimeException("Batch full");
		}
		records.add(size + 1, record);
		size++;
	}

	public int getStartOffset() {
		return startOffset;
	}

	public int getCapacity() {
		return capacity;
	}

	public boolean isFull() {
		return size == capacity;
	}

	public int size() {
		return size;
	}

	public boolean isEmpty() {
		return size == 0;
	}

	public Record getRecord(int index) {
		if (index < 0 || index >= size) {
			throw new RuntimeException("Index out of bounds");
		}
		return records.get(index);
	}

	public boolean isExpired() {
		return !isEmpty() && records.get(size - 1).isExpired();
	}
	
	public String convertToLog() {
		StringBuilder log = new StringBuilder("");
		for (Record record: records) {
			log.append(GsonUtils.getGson().toJson(record));
			log.append("\n");
		}
		return log.toString();
	}
}
