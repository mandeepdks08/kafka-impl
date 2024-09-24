package com.kafka.restmodel;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class ConsumerRegistrationRequest {
	private String topic;
	private String consumerId;
}
