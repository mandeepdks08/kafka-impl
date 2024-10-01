package com.kafka.restmodel;

import lombok.Getter;
import lombok.Setter;
import lombok.experimental.SuperBuilder;

@Getter
@Setter
@SuperBuilder
public class BaseResponse {
	private String message;
	private Boolean success;
}
