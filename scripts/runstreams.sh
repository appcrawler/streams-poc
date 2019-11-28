jps | grep streams | awk '{print $1}' | xargs kill -9
rm -rf streams-state-store*
java -Dschema-file=schema.avsc -Dstreams-properties=streams.properties -Djava.util.logging.config.file=log.properties -jar ../target/streams-poc-1.0-shaded.jar >  streams.out &
java -Dschema-file=schema.avsc -Dstreams-properties=streams2.properties -Djava.util.logging.config.file=log.properties -jar ../target/streams-poc-1.0-shaded.jar > streams2.out &
java -Dschema-file=schema.avsc -Dstreams-properties=streams3.properties -Djava.util.logging.config.file=log.properties -jar ../target/streams-poc-1.0-shaded.jar > streams3.out &

