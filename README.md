# Kafka_twitter_mongodb

A data pipeline is built that extracts twitter stream data, processes using Kafka, stores in mongodb

the stream data is passed to the kafka producer topic, then consumer consumes data then the data is sent to the mongodb.

# Executing Files

1) Start zookeeper and kafka server
2) Run the KafkaProducer.py file
3) Run the KafkaConsumerMongo.py file

## NOTE: create kafka topic, mongodb database and collection first before running the files
