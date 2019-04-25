package com.example.demos3reactive;


import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.Bucket;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.zip.GZIPOutputStream;

@Testcontainers
public class AsyncClientTest {
    @Container
    static GenericContainer minio = new GenericContainer("minio/minio:RELEASE.2019-04-04T18-31-46Z")
            .withEnv("MINIO_ACCESS_KEY","admin")
            .withEnv("MINIO_SECRET_KEY","password")
            .withEnv("MINIO_REGION","eu-central-1")
            .withExposedPorts(9000)
            .withCommand("server","/data");

    static int minioPort;
    static S3Client syncClient;
    static S3AsyncClient asyncClient;

    @BeforeAll
    static void beforeAll() {
        minioPort = minio.getMappedPort(9000);
        syncClient = S3Client.builder()
                .region(Region.EU_CENTRAL_1)
                .endpointOverride(URI.create("http://localhost:" + minioPort))
                .credentialsProvider(() -> AwsBasicCredentials.create("admin","password"))
                .serviceConfiguration(sb -> sb.pathStyleAccessEnabled(true))
                .build();

        syncClient.createBucket(cb -> cb.bucket("test"));

        syncClient.listBuckets().buckets().stream().map(Bucket::name).forEach(System.out::println);

        asyncClient = S3AsyncClient.builder()
                .region(Region.EU_CENTRAL_1)
                .endpointOverride(URI.create("http://localhost:" + minioPort))
                .credentialsProvider(() -> AwsBasicCredentials.create("admin","password"))
                .serviceConfiguration(sb -> sb.pathStyleAccessEnabled(true))
                .build();

    }

    @Test
    public void head_request_of_compressed_content() throws Exception {
        String payload = "Hello, world!";
        byte [] compressedPayload = compress(payload);
        PutObjectRequest por = PutObjectRequest.builder()
                .bucket("test")
                .contentEncoding("gzip")
                .contentLength((long)payload.length())
                .contentType("text/plain")
                .key("foo")
                .build();
        syncClient.putObject(por, RequestBody.fromBytes(compressedPayload));


        HeadObjectRequest hor = HeadObjectRequest.builder()
                .key("foo")
                .bucket("test")
                .build();
        asyncClient.headObject(hor).get();
    }

    public static byte[] compress(String data) throws IOException {
        ByteArrayOutputStream bos = new ByteArrayOutputStream(data.length());
        GZIPOutputStream gzip = new GZIPOutputStream(bos);
        gzip.write(data.getBytes(StandardCharsets.UTF_8));
        gzip.close();
        byte[] compressed = bos.toByteArray();
        bos.close();
        return compressed;
    }
}
