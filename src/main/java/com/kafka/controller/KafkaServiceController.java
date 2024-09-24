package com.kafka.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;

import com.kafka.restmodel.BaseResponse;
import com.kafka.restmodel.CreateTopicRequest;
import com.kafka.service.KafkaService;

@Controller
@RequestMapping("/kafka")
public class KafkaServiceController {

	@Autowired
	private KafkaService kafkaService;

	@RequestMapping(value = "/create-topic", method = RequestMethod.POST)
	protected ResponseEntity<BaseResponse> createTopic(@RequestBody CreateTopicRequest createTopicRequest) {
		String topicName = createTopicRequest.getTopic();
		Integer partitions = createTopicRequest.getPartitions();
		Integer ttlInSeconds = createTopicRequest.getTtlInSeconds();
		kafkaService.createTopic(topicName, partitions, ttlInSeconds);
		return new ResponseEntity<>(BaseResponse.builder().message("Topic created").success(true).build(),
				HttpStatus.OK);
	}
}
