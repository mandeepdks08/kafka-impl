package com.kafka.datamodel;

import java.time.LocalDateTime;

public class Record {
	private String data;
	private Record next;
	private LocalDateTime expiration;

	public Record(String data, Integer ttlInSeconds) {
		this.data = data;
		this.expiration = LocalDateTime.now().plusSeconds(ttlInSeconds);
	}

	public Record(String data, Record next, Integer ttlInSeconds) {
		this.data = data;
		this.next = next;
		this.expiration = LocalDateTime.now().plusSeconds(ttlInSeconds);
	}

	public String getData() {
		return data;
	}

	public Record getNext() {
		return next;
	}

	public void setNext(Record next) {
		this.next = next;
	}
	
	public boolean isExpired() {
		return LocalDateTime.now().isAfter(expiration);
	}
}
