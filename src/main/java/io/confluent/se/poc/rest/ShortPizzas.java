package io.confluent.se.poc.rest;

import org.apache.kafka.common.serialization.*;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.config.ConfigException;
import java.io.*;
import java.time.Duration;
import java.util.*;

import org.json.*;

public class ShortPizzas {
  static KafkaConsumer<String, Object> c;

  public static void main(String args[]) throws Exception {
    Properties props = new Properties();

    InputStream input = new FileInputStream("props.properties");
    //InputStream input = new FileInputStream("cloudprops.properties");
    props.load(input);
    //props.put(ConsumerConfig.GROUP_ID_CONFIG, "DemoConsumer");
    props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
    props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
    props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "30000");
    props.put("value.deserializer", "io.confluent.kafka.serializers.KafkaAvroDeserializer");
    props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
    //props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

    c = new KafkaConsumer<String, Object>(props);
    String topic = (String)props.get("topic");
    c.subscribe(Collections.singletonList(topic));
    System.out.println("subscribed to topic " + topic);
    while (true) {
      //System.out.println("trying to get records...");
      ConsumerRecords<String, Object> records = c.poll(Duration.ofSeconds(1));
      //System.out.println(records.toString());
      for (ConsumerRecord<String, Object> record : records) {

        String s = record.value().toString();
        JSONObject j = new JSONObject(s);
        System.out.println("Transaction ID " + record.key() + " only took " + Integer.parseInt(j.get("COOK_TIME").toString())/1000 + " seconds to cook.  It's either vastly underdone or we're selling old pizzas :)");
        //System.out.println("Received messagid (" + record.key() + ", " + record.value().toString() + ") at offset " + record.offset());
      }
      //Thread.sleep(3000);
    }
  }
}
