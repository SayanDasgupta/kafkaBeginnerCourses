package com.sayan.kafkaBasics.demos.kafka;

import java.util.Properties;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProducerDemoWithCallback {
	
	private static final Logger log = LoggerFactory.getLogger(ProducerDemoWithCallback.class.getSimpleName()); 
	
	public static void main(String[] args) {
		log.info("Inside ProducerDemo With Callback");
		
		// create Producer Properties
		Properties properties = new Properties();
		properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
		properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		
		// create Producer
		KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);
		
		// create a Producer Record : actual data to be sent
		ProducerRecord<String, String> producerRecord = new ProducerRecord<String, String>("demo_java", "hello world2");
		
		// send data - asynchronous
		producer.send(producerRecord, new Callback() {
			
			public void onCompletion(RecordMetadata metadata, Exception e) {
				// executes everytime a record is successfully inserted or Exception is thrown
				
				if(e==null) {
					// record send is success
					log.info(
								"Recieved new Metadata/ \n" +
								"Topic: " + metadata.topic() + "\n" +
								"Partitions: " + metadata.partition() + "\n" +
								"Offsets: " + metadata.offset() + "\n" +
								"TimeStamp: " + metadata.timestamp()  + "\n"
							);
				} else {
					log.error("Exception During Producing to Kafka: ", e);
				}
			}
		});
		
		// flush data - synchronous
		producer.flush();
		
		// flush and close the producer
		producer.close();						// This call to close() function both flushes the data from the producer to the kafka server and closes the producer  
		
		
		
		log.info("Finsished The ProducerDemo Class");
	}

}
