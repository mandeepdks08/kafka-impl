package com.kafka.config;

import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ControllerAdvice;
import org.springframework.web.bind.annotation.ExceptionHandler;

import com.kafka.restmodel.BaseResponse;

@ControllerAdvice
public class GlobalExceptionHandler {

	@ExceptionHandler(RuntimeException.class)
	public ResponseEntity<BaseResponse> handleRuntimeException(RuntimeException e) {
		return new ResponseEntity<>(BaseResponse.builder().message(e.getMessage()).success(false).build(),
				HttpStatus.BAD_REQUEST);
	}
}
