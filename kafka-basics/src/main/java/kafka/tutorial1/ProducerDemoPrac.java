package kafka.tutorial1;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemoPrac {
    public static void main(String[] args) {

        Logger logger = LoggerFactory.getLogger(ProducerDemoPrac.class);

        String bootStrapServer = "127.0.0.1:9092";
        String topic = "first_topic";

        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootStrapServer);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                StringSerializer.class.getName());

        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

        for (int i = 0; i < 10; i++) {
            String key = "Id_" + i;

            logger.info("Key is : {}", key);

            ProducerRecord<String, String> records = new ProducerRecord<String, String>(topic, key,
                    "HelloWorld");

            //producer.send(records);

            producer.send(records, new Callback() {

                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    if (e == null) {
                        logger.info("Topic is : " + recordMetadata.topic());
                        logger.info("\n Partition Number is : " + recordMetadata.partition());
                        logger.info("\n Offset is : " + recordMetadata.offset());
                        logger.info("\n Timestamp is : " + recordMetadata.timestamp());
                    } else {
                        e.printStackTrace();
                        logger.error("Error while producing : ", e);
                    }
                }
            });


        }
        producer.flush();
        producer.close();
    }
}
