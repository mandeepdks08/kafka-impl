package com.kafka.datamodel;

import java.time.LocalDateTime;

public class Record {
	private String data;
	private LocalDateTime expiration;

	public Record(String data, Integer ttlInSeconds) {
		this.data = data;
		this.expiration = LocalDateTime.now().plusSeconds(ttlInSeconds);
	}

	public Record(String data, Record next, Integer ttlInSeconds) {
		this.data = data;
		this.expiration = LocalDateTime.now().plusSeconds(ttlInSeconds);
	}

	public String getData() {
		return data;
	}

	public boolean isExpired() {
		return LocalDateTime.now().isAfter(expiration);
	}
}
