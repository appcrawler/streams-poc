package io.confluent.se.poc.streams;

import io.confluent.kafka.streams.serdes.avro.*;

import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.common.serialization.*;
import org.apache.kafka.streams.state.*;
import org.apache.avro.generic.*;
import org.apache.avro.*;

import io.confluent.kafka.serializers.*;

import org.apache.http.HttpEntity;
import org.apache.http.HttpHeaders;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;

import java.io.*;
import java.util.*;
import java.util.concurrent.CompletableFuture;

import io.confluent.se.poc.rest.ApiServer;
import io.confluent.se.poc.utils.PocData;

public class PocKafkaStreams {
  static ReadOnlyKeyValueStore<String, GenericRecord> kvStore;
  static CompletableFuture<KafkaStreams.State> stateFuture;
  static KafkaStreams streams;
  static StreamsMetadata metadata;
  static HostInfo hinfo;

  public static void main(String[] args) throws Exception {
    PocKafkaStreams pks = new PocKafkaStreams();
    pks.start();
  }

  public void start() throws Exception {
    Properties props = new Properties (); 
    props.load(new java.io.FileInputStream(System.getProperty("streams-properties")));

    ApiServer restServer = new ApiServer(props.get("api.port").toString());
    hinfo = new HostInfo("localhost",Integer.parseInt(props.get("api.port").toString()));
    restServer.start();

    /*******************************************************************************************************
    This section simply builds a schema for our output topic.  This is automatically handled in KSQL
    *******************************************************************************************************/

    InputStream is = new FileInputStream(props.get("schema.file").toString());
    BufferedReader buf = new BufferedReader(new InputStreamReader(is));
    String line = buf.readLine();
    StringBuilder sb = new StringBuilder();
    while(line != null){
      sb.append(line).append("\n");
      line = buf.readLine();
    }
    String streamSchema = sb.toString();

    /*******************************************************************************************************
    This section sets up our serializer class for our output.  This is automatically handled in KSQL
    We have commented it out, as we are relying on the default serdes of string and avro generic records.
    KSQL, to which we would like to move what is below, only (currently) support string based keys.  We
    chose Avro just because it is compact, binary, and schema based. 
    *******************************************************************************************************/

/* - Uncomment if you would like to use serdes different from the default described above
    final Map<String, String> serdeConfig = Collections.singletonMap("schema.registry.url",props.get("schema.registry.url").toString());
    final Serde<String> keyGenericAvroSerde = Serdes.String();
    keyGenericAvroSerde.configure(serdeConfig, true); // `true` for record keys
    final Serde<GenericRecord> valueGenericAvroSerde = new GenericAvroSerde();
    valueGenericAvroSerde.configure(serdeConfig, false); // `false` for record values
*/

    final Map<String, String> serdeConfig = Collections.singletonMap("schema.registry.url",props.get("schema.registry.url").toString());
    final Serde<GenericRecord> keyGenericAvroSerde = new GenericAvroSerde();
    keyGenericAvroSerde.configure(serdeConfig, true); // `true` for record keys
    final Serde<GenericRecord> valueGenericAvroSerde = new GenericAvroSerde();
    valueGenericAvroSerde.configure(serdeConfig, false); // `false` for record values

    /*******************************************************************************************************
    This section begins setting up our streams (topics) to join for output.  This is automatically handled 
    in KSQL by virtue of the SQL JOIN semantics
    *******************************************************************************************************/

    StreamsBuilder builder = new StreamsBuilder();
    String stateStore = props.get("state.store.name").toString();
    KeyValueBytesStoreSupplier storeSupplier = Stores.persistentKeyValueStore(stateStore);

    //serdes different from the default
    //KStream<String, GenericRecord> customers = builder.stream("customers_stream", Consumed.with(Serdes.String(), valueGenericAvroSerde));
    //default serdes
    KStream<String, GenericRecord> customers = builder.stream("customers_stream");

    //serdes different from the default
    //KStream<String, GenericRecord> orders = builder.stream("orders_s", Consumed.with(Serdes.String(), valueGenericAvroSerde));
    //default serdes
    KStream<String, GenericRecord> orders = builder.stream("orders_s");

    //repartition orders stream to key off of customer_id so we can join it to the customers stream
    KStream<String, GenericRecord> orders_keyed_by_customer_id = orders.selectKey((k,v) -> v.get("customer_id").toString());

    //serdes different from the default
    //orders_keyed_by_customer_id.to("orders_rekey_s", Produced.with(Serdes.String(), valueGenericAvroSerde));
    //default serdes
    orders_keyed_by_customer_id.to("orders_rekey_s");

    //serdes different from the default
    //customers.to("customers_table",Produced.with(Serdes.String(), valueGenericAvroSerde));
    //default serdes
    customers.to("customers_table");

    //serdes different from the default
/*
    System.out.println("STATE STORE NAME = " + stateStore);
    KeyValueBytesStoreSupplier storeSupplier = Stores.persistentKeyValueStore(stateStore);
    System.out.println("GOT STATE STORE NAME = " + stateStore);

    KStream<String, GenericRecord> customers = builder.stream("customers_stream", Consumed.with(Serdes.String(), valueGenericAvroSerde));
    KStream<String, GenericRecord> orders = builder.stream("orders_stream", Consumed.with(Serdes.String(), valueGenericAvroSerde));

    customers.to("customers_table",Produced.with(Serdes.String(), valueGenericAvroSerde));
    KTable<String, GenericRecord> materializedCustomers = builder.table("customers_table",
                                                                 Materialized.<String, GenericRecord>as(storeSupplier)
                                                                     .withKeySerde(Serdes.String())
                                                                     .withValueSerde(valueGenericAvroSerde)                           
                                                               );
*/

    //default serdes
    KTable<String, GenericRecord> materializedCustomers = builder.table("customers_table",
                                                                 Materialized.<String, GenericRecord>as(storeSupplier)
                                                               );
    /*******************************************************************************************************
    This section actually joins the two streams, and importantly, creates and returns a new Avro
    GenericRecord for output to our stream that will serve the joined events.  
    The requirement is that both topics are partitioned on the same key.
    *******************************************************************************************************/

    KStream<String, GenericRecord> joined = orders_keyed_by_customer_id.join(materializedCustomers,
                                                       (leftValue, rightValue) -> {
                                                            Schema.Parser parser = new Schema.Parser();
                                                            Schema schema = parser.parse(streamSchema);
                                                            GenericRecord avroRecord = new GenericData.Record(schema);
                                                            avroRecord.put("order_id", leftValue.get("order_id"));
                                                            avroRecord.put("sku", leftValue.get("sku"));
                                                            avroRecord.put("price", (Integer)leftValue.get("price"));
                                                            avroRecord.put("quantity", (Integer)leftValue.get("quantity"));
                                                            avroRecord.put("first_name", rightValue.get("first_name"));
                                                            avroRecord.put("last_name", rightValue.get("last_name"));
                                                            return avroRecord;
                                                        });

    /*******************************************************************************************************
    This section simply prints to standard output each record joined.  Comment this line if you don't want
    to view this, or add a debug flag
    *******************************************************************************************************/

    joined.foreach((k,v) -> System.out.println("Key = " + k + ", Value = " + v));
    
    /*******************************************************************************************************
    This section actually published the joined events above to our new stream, perhaps to be consumed by
    another microservice, database, dashboard, etc.
    *******************************************************************************************************/

    //serdes different from the default
    //joined.to("orders_customers_stream",Produced.with(Serdes.String(), valueGenericAvroSerde));
    //default serdes
    joined.to("orders_customers_stream");

    /*******************************************************************************************************
    This is where the application topology above is actually physically started
    *******************************************************************************************************/

    Topology topology = builder.build();
    streams = new KafkaStreams (topology, props); 
    System.out.println("created KafkaStreams");

    this.stateFuture = new CompletableFuture<>();
    streams.setStateListener((newState, oldState) -> {
      if(stateFuture.isDone()) {
        return;
      }

      if(newState == KafkaStreams.State.RUNNING || newState == KafkaStreams.State.ERROR) {
        stateFuture.complete(newState);
      }
    });

    streams.start();

    while (stateFuture.get() != KafkaStreams.State.RUNNING) {}

    System.out.println("DIAGRAM " + streams.localThreadsMetadata());
    System.out.println(stateFuture.get());
    System.out.println("METADATA = " + streams.allMetadata());

    Runtime.getRuntime().addShutdownHook(new Thread(() -> {
      streams.close(); 
    }));

    /*******************************************************************************************************
    This allows us to serve interactive queries to our application.  This will be available in December 2019
    in KSQL, but only for aggregations such as SUM(), COUNT(), etc.  Getting a simple customer by customer_id
    will not be available, initially.
    in KSQL.
    *******************************************************************************************************/

    while (true) {
      try {
        kvStore = streams.store(stateStore, QueryableStoreTypes.keyValueStore());
      } 
      catch (org.apache.kafka.streams.errors.InvalidStateStoreException ignored) {
        Thread.sleep(1000);
        System.out.println("Waiting for store to become ready...");
      }
    }
  }

  public static String getCustomers(String id) throws Exception {
    metadata = streams.metadataForKey("OrderCustomerStore", id, Serdes.String().serializer());
    System.out.println(hinfo + " " + id);
    metadata = streams.metadataForKey("OrderCustomerStore", id, Serdes.String().serializer());
    System.out.println(metadata.hostInfo() + " " + hinfo + " " + id);
    try {
      if (metadata.hostInfo().equals(hinfo)) {
        System.out.println("found local! " + hinfo + " " + id);
        return kvStore.get(id).toString();
      }
      else {
        System.out.println("getting from " + hinfo + " " + id);
        return getRemote(metadata, id);
      }
    }
    catch (Exception e) {
      System.out.println(stateFuture.get());
      throw(e);
    }
  }

  public static String getRemote(StreamsMetadata metadata,String id) throws Exception {
    CloseableHttpClient httpClient = HttpClients.createDefault();
    System.out.println("getting record");
    System.out.println("getting record from " + metadata.port());
    HttpGet request = new HttpGet("http://" + metadata.host() + ":" + metadata.port() + "/api/customer/" + id);
    CloseableHttpResponse response = httpClient.execute(request);
    HttpEntity entity = response.getEntity();
    String result = "";
    if (entity != null) {
      result = EntityUtils.toString(entity);
      System.out.println(result);
    }
    httpClient.close();
    return result;
  }
}
