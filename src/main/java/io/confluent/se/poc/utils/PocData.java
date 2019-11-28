package io.confluent.se.poc.utils;

import java.util.Properties;
import org.apache.kafka.clients.producer.*;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import java.io.*;
import java.util.*;

import org.json.*;

public class PocData {
  public static void main(String[] args) throws Exception{
    PocData pd = new PocData();
    pd.loadOrders();
    pd.loadCustomers();
  }

  public void loadOrders() throws Exception {
    System.out.println("loading orders...");
    Producer<String, GenericRecord> producer = null;
    try {

      String topicName = "orders_s";
        
      Properties props = new Properties();
      props.load(new java.io.FileInputStream(System.getProperty("streams-properties")));

      props.put("bootstrap.servers","localhost:9092");
      props.put("acks", "all");
      props.put("retries", 0);
      props.put("key.serializer","org.apache.kafka.common.serialization.StringSerializer");
      props.put("value.serializer","io.confluent.kafka.serializers.KafkaAvroSerializer");

      producer = new KafkaProducer<String, GenericRecord>(props);
              
      int i = 0;
      Random r = new Random();
      InputStream is = new FileInputStream(props.get("orders.schema.file").toString());
      BufferedReader buf = new BufferedReader(new InputStreamReader(is));
      String line = buf.readLine();
      StringBuilder sb = new StringBuilder();
      while(line != null){
        sb.append(line).append("\n");
        line = buf.readLine();
      }
      String schemaString = sb.toString();
      Schema schema = new Schema.Parser().parse(schemaString);
      int j = 0;
      while (i++ <= 1000) {
        GenericData.Record gr = new GenericData.Record(schema);
        String order_id = "OH";
        gr.put("order_id","ORD" + i);
        j = j++ > 20 ? 1 : j;
        gr.put("customer_id","CUST" + j);
        gr.put("sku","SKU" + i);
        gr.put("quantity", i);
        gr.put("price", i);

        ProducerRecord record = new ProducerRecord<String, GenericRecord>(topicName,null,System.currentTimeMillis(),
                                                                   "ORD" + i,
                                                                   gr
                                                                  );
        producer.send(record, new Callback() {
          public void onCompletion(RecordMetadata metadata, Exception e) {
            if (e != null)
              e.printStackTrace();
          }
        });
      }
    }
    finally {
      try {
        producer.close();
      }
      catch (Exception e) {
        //ignore
      }
    }
  }

  public void loadCustomers() throws Exception{
    System.out.println("loading customers...");
    Producer<String, GenericRecord> producer = null;
    try {

      String topicName = "customers_stream";
        
      Properties props = new Properties();
      props.load(new java.io.FileInputStream(System.getProperty("streams-properties")));

      String[] states = {"OH","KY","WV","VA","NC","SC","TN","AL","GA","AL","PA","FL"};
      String[] genders = {"M","F"};
      String[] fnames = {"Mark","Ben","John","Jim","Fred","Becky","Abby","Maddie","Jenna","Eileen","Marie","Olivia"};
      String[] lnames = {"Kosar","Lynn","Rice","Warner","Linkowicz","Desseau","Lucas","Koch","Pickens","Hilton","Graves","Shepard"};
  
      props.put("bootstrap.servers","localhost:9092");
      props.put("acks", "all");
      props.put("retries", 0);
      props.put("key.serializer","org.apache.kafka.common.serialization.StringSerializer");
      props.put("value.serializer","io.confluent.kafka.serializers.KafkaAvroSerializer");

      producer = new KafkaProducer<String, GenericRecord>(props);
              
      int i = 0;
      Random r = new Random();
      InputStream is = new FileInputStream(props.get("customers.schema.file").toString());
      BufferedReader buf = new BufferedReader(new InputStreamReader(is));
      String line = buf.readLine();
      StringBuilder sb = new StringBuilder();
      while(line != null){
        sb.append(line).append("\n");
        line = buf.readLine();
      }
      String schemaString = sb.toString();
      Schema schema = new Schema.Parser().parse(schemaString);
      while (i++ <= 20) {
        GenericData.Record gr = new GenericData.Record(schema);
        gr.put("customer_id","CUST" + i);
        gr.put("first_name",fnames[r.nextInt(fnames.length - 1)]);
        gr.put("last_name",lnames[r.nextInt(lnames.length - 1)]);
        gr.put("age", i);
        gr.put("gender",genders[r.nextInt(genders.length - 1)]);
        gr.put("state",states[r.nextInt(states.length - 1)]);

        ProducerRecord record = new ProducerRecord<String, GenericRecord>(topicName,null,System.currentTimeMillis(),
                                                                   "CUST" + i,
                                                                   gr
                                                                  );
        producer.send(record, new Callback() {
          public void onCompletion(RecordMetadata metadata, Exception e) {
            if (e != null)
              e.printStackTrace();
          }
        });
      }
    }
    finally {
      try {
        producer.close();
      }
      catch (Exception e) {
        //ignore
      }
    }
  }
}
