package io.confluent.se.poc.streams;

import io.confluent.kafka.streams.serdes.avro.*;

import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.common.serialization.*;
import org.apache.kafka.streams.state.*;
import org.apache.avro.generic.*;
import org.apache.avro.*;

import org.json.*;

import io.confluent.kafka.serializers.*;

//import java.util.*;
import java.io.*;
import java.util.concurrent.CompletableFuture;

public class AvroUtil {
  public static GenericRecord transform(GenericRecord value1, GenericRecord value2) {
    try {
      InputStream is = new FileInputStream(System.getProperty("schema-file")); 
      BufferedReader buf = new BufferedReader(new InputStreamReader(is)); 
      String line = buf.readLine(); 
      StringBuilder sb = new StringBuilder(); 
      while(line != null){ 
        sb.append(line).append("\n"); 
        line = buf.readLine(); 
      } 
      String streamSchema = sb.toString();
  
      Schema.Parser parser = new Schema.Parser();
      Schema schema = parser.parse(streamSchema);
      GenericRecord avroRecord = new GenericData.Record(schema);
      avroRecord.put("order_id", value1.get("order_id"));
      avroRecord.put("sku", value1.get("sku"));
      avroRecord.put("price", (Integer)value1.get("price"));
      avroRecord.put("quantity", (Integer)value1.get("quantity"));
      avroRecord.put("first_name", value2.get("first_name"));
      avroRecord.put("last_name", value2.get("last_name"));
      return avroRecord;
    } 
    catch (Exception e) {
      e.printStackTrace();
      return null;
    }
  }
}
