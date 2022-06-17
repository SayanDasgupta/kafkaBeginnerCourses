package com.sayan.kafkaBasics.demos.kafka;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProducerDemo {
	
	private static final Logger log = LoggerFactory.getLogger(ProducerDemo.class.getSimpleName()); 
	
	public static void main(String[] args) {
		log.info("Inside ProducerDemo");
		
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
		producer.send(producerRecord);
		
		// flush data - synchronous
		producer.flush();
		
		// flush and close the producer
		producer.close();						// This call to close() function both flushes the data from the producer to the kafka server and closes the producer  
		
		
		
		log.info("Finsished The ProducerDemo Class");
	}

}
