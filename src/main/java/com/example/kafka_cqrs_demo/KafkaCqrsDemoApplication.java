package com.example.kafka_cqrs_demo;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.ComponentScan;

@SpringBootApplication
@ComponentScan(basePackages = "com.example.kafka_cqrs_demo")
public class KafkaCqrsDemoApplication {

	public static void main(String[] args) {
		SpringApplication.run(KafkaCqrsDemoApplication.class, args);
	}

}
