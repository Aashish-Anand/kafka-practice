package kafka.tutorial1;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class ProducerDemoKeys {

    /*
        We add the key with the message this will make sure that every time you
        are running this program the same key will always go to the same partition
     */
    public static void main(String[] args) throws ExecutionException, InterruptedException {

        final Logger logger = LoggerFactory.getLogger(ProducerDemoKeys.class);
        /*
            Steps to start producing the data
            1. Create producer properties
            2. Create the producer
            3. Send data
         */

        String bootstrapServers = "127.0.0.1:9092";

        // Producer Properties
        /* Old way
            Properties properties = new Properties();
            properties.setProperty("boostrap.servers", bootstrapServers);
            properties.setProperty("key.serializer", StringSerializer.class.getName()); //this is
             for string
            serializer as we are sending the data of type String
            properties.setProperty("value.seralizer", StringSerializer.class.getName());
        */
        //New way
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                StringSerializer.class.getName());

        // create the producer
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

        for (int i = 0; i < 10; i++) {

            String topic = "first_topic";
            String value = "hello world" + Integer.toString(i);
            String key = "id_" + Integer.toString(i);


            // create a producer record
            ProducerRecord<String, String> record = new ProducerRecord<String, String>(topic, key,
                    value);

            logger.info("Key : " + key);
            //id_0 at partition 1
            //id_1 at partition 0
            //id_2 at partition 2
            //id_3 at partition 0
            //id_4 at partition 2
            //id_5 at partition 2
            //id_6 at partition 0
            //id_7 at partition 2
            //id_8 at partition 1
            //id_9 at partition 2


            //sendData - sync
            producer.send(record, new Callback() {
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    //executes every time when record is successfully sent or an exception is thrown
                    if (e == null) {
                        logger.info("Received new Metadata. \n" +
                                "Topic : " + recordMetadata.topic() + "\n" +
                                "Partition : " + recordMetadata.partition() + "\n" +
                                "Offset : " + recordMetadata.offset() + "\n" +
                                "Timestamp : " + recordMetadata.timestamp());
                    } else {
                        e.printStackTrace();
                        logger.error("Error while producing : {}", e);
                    }
                }
            }).get(); // block the .send() to make it synchronous - dont do this in production!
            // by using the .get() the message will be send 1 by 1
        }
        // flush data
        producer.flush();
        // flush and close producer
        producer.close();
    }
}
