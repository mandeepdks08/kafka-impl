package com.kafka.restmodel;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class CreateTopicRequest {
	private String topic;
	private Integer partitions;
	private Integer ttlInSeconds;
}
