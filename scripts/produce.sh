java -Dstreams-properties=streams.properties \
     -Dorders.schema.file=orders.avsc \
     -Dcustomers.schema.file=customer.avsc \
     -Dlog4j.configuration=../log.properties \
     -cp ../target/streams-poc-1.0-shaded.jar io.confluent.se.poc.utils.PocData
