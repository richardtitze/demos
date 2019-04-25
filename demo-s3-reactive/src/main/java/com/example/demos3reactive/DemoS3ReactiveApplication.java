package com.example.demos3reactive;

import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;

@SpringBootApplication
public class DemoS3ReactiveApplication implements CommandLineRunner {

	public static void main(String[] args) {
		SpringApplication.run(DemoS3ReactiveApplication.class, args);
	}

	@Override
	public void run(String... args) throws Exception {
		S3AsyncClient client = S3AsyncClient.builder()
				.serviceConfiguration(builder -> builder.checksumValidationEnabled(false))
				.build();
		HeadObjectRequest hor = HeadObjectRequest.builder()
				.bucket("bucket")
				.key("key") // object stored has Content-Encoding: gzip
				.build();
		client.headObject(hor).get();
	}
}
