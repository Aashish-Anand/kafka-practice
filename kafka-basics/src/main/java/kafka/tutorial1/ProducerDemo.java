package kafka.tutorial1;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class ProducerDemo {

    public static void main(String[] args) {
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

        // create a producer record
        ProducerRecord<String, String> record = new ProducerRecord<String, String>("first_topic",
                "hello_world");

        //sendData - async
        producer.send(record);

        // flush data
        producer.flush();
        // flush and close producer
        producer.close();
    }
}
