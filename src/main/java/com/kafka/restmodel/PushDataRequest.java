package com.kafka.restmodel;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class PushDataRequest {
	private String topic;
	private String key;
	private String data;
}
