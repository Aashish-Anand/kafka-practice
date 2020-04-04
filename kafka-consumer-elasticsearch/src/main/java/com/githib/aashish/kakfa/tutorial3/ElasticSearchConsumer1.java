package com.githib.aashish.kakfa.tutorial3;

import com.google.gson.JsonParser;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ElasticSearchConsumer1 {

    public static RestHighLevelClient createClient() {
        
        String hostname = "";
        String username = "";
        String password = "";

        final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(username,
                password));

        /*
        Below lines means that connect to given hostname over this port : 443 and https
        with above credentials
         */
        RestClientBuilder builder = RestClient.builder(
                new HttpHost(hostname, 443, "https"))
                .setHttpClientConfigCallback(new RestClientBuilder.HttpClientConfigCallback() {
                    @Override
                    public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpAsyncClientBuilder) {
                        return httpAsyncClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
                    }
                });

        RestHighLevelClient client = new RestHighLevelClient(builder);
        return client;


    }

    public static KafkaConsumer<String, String> createConsumer(String topic) {
        String bootstrapServers = "127.0.0.1:9092";
        String groupId = "kafka-demo-elasticsearch1";

        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");  // disable auto commit
        // of offset
        properties.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "50"); // we will receive only 10
        // records at a time

        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);
        consumer.subscribe(Arrays.asList(topic));

        return consumer;
    }

    public static void main(String[] args) throws IOException {

        Logger logger = LoggerFactory.getLogger(ElasticSearchConsumer.class);
        RestHighLevelClient client = createClient();

        String topic = "twitter_tweets";
        KafkaConsumer<String, String> consumer = createConsumer(topic);

        while (true) {
            ConsumerRecords<String, String> records =
                    consumer.poll(Duration.ofMillis(100)); // new in kafka 2.0.0

            int recordCounts = records.count();
            logger.info("Received " + recordCounts + " records");


            BulkRequest bulkRequest = new BulkRequest();

            for (ConsumerRecord<String, String> record : records) {
                //logger.info(record.value());
                String jsonString = record.value();

                // 2 Strategies to generate ID
                //1. kakfa generic id
                //String id = record.topic()+"_"+record.partition()+"_"+record.partition();

                //2. twitter feed specfic id :
                try {
                    String id = extractIdFromTweets(record.value());

                    IndexRequest indexRequest = new IndexRequest(
                            "twitter",
                            "tweets",
                            id   // introducing the id to make the consumer idempotent
                    ).source(jsonString, XContentType.JSON);

                    //uncomment if you want to add each record a timme
//                IndexResponse indexResponse = client.index(indexRequest, RequestOptions.DEFAULT);
//
//                logger.info(indexResponse.getId());
//
//                try {
//                    Thread.sleep(10);
//                } catch (InterruptedException e) {
//                    e.printStackTrace();
//                }

                    // for bulk
                    bulkRequest.add(indexRequest); //  we add to our bulk request(takes no time)
                } catch (NullPointerException e) {
                    logger.warn("skipping bad data : "+ record.value());
                }

            }

            if (recordCounts > 0) {
                // for bulk
                BulkResponse bulkItemResponses = client.bulk(bulkRequest, RequestOptions.DEFAULT);

                logger.info("Committing the message.");
                consumer.commitSync();  // we are commiting manually after we all data is processed
                logger.info("Offsets have been committed");
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }

        //closing the client
        //client.close();
    }

    private static JsonParser jsonParser = new JsonParser();

    private static String extractIdFromTweets(String tweetJson) {
        /*
        What we are doing here is that we are using twitter itself id as our
        index id so that already consumed mesage will not be handled again

         */
        //gson library to parse the string to json
        return jsonParser.parse(tweetJson)
                .getAsJsonObject()
                .get("id_str")
                .getAsString();
    }
}
