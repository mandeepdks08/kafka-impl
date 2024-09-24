package com.kafka.datamodel;

import lombok.Builder;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
@Builder
public class ConsumerRecord {
	private String data;
	private String consumerId;
	private String topic;
	private Integer partitionIndex;
}
