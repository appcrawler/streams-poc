#---------------------------------------------------------------------------------
# Stop anything currently running on this host and delete for fresh start
jps | grep streams | awk '{print $1}' | xargs kill -9
rm -rf /root/streams/scripts/streams-state-store*
confluent local stop
rm -rf /tmp/confluent.*
rm -rf /var/lib/kafka
#---------------------------------------------------------------------------------

#---------------------------------------------------------------------------------
# sleep five seconds to ensure everything to eliminate startup errors

rm -rf /root/streams/scripts/streams-state-store*

sleep 5
confluent local start
#---------------------------------------------------------------------------------

#---------------------------------------------------------------------------------
# create topics for orders, customers, and pizze status, as well as predefined topics
# to hold our KSQL streams and tables

kafka-topics --bootstrap-server localhost:9092 --create --topic customers_stream --partitions 2 --replication-factor 1
kafka-topics --bootstrap-server localhost:9092 --create --topic customers_table --partitions 2 --replication-factor 1 --config "cleanup.policy=compact"
kafka-topics --bootstrap-server localhost:9092 --create --topic orders_stream --partitions 2 --replication-factor 1
kafka-topics --bootstrap-server localhost:9092 --create --topic orders_rekey_s --partitions 2 --replication-factor 1
kafka-topics --bootstrap-server localhost:9092 --create --topic orders_customers_stream --partitions 2 --replication-factor 1
#---------------------------------------------------------------------------------

#---------------------------------------------------------------------------------
./runstreams.sh
java -Dstreams-properties=streams.properties \
     -Dorders.schema.file=orders.avsc \
     -Dcustomers.schema.file=customer.avsc \
     -Dlog4j.configuration=../log.properties \
     -cp ../target/streams-poc-1.0-shaded.jar io.confluent.se.poc.utils.PocData
echo "RUN SCRIPT 'stream.ksql';" | ksql
#---------------------------------------------------------------------------------
