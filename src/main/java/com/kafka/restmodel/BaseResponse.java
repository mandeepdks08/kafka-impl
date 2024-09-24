package com.kafka.restmodel;

import lombok.Builder;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
@Builder
public class BaseResponse {
	private String message;
	private boolean success;
}
