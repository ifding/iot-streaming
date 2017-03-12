# iot-streaming
IoT Streaming using Flink to connect Kafka and Cassandra, Elastic

* Import in IntelliJ

	* IntelliJ:
		* Select “File” -> “Import Project”
		* Select root folder of your project
		* Select “Import project from external model”, select “Maven”
		* Leave default options and finish the import
      
* How to run this project

1. Using the `kafka/Producer.java` to simulate the IoT Data Producer

2. You can choose to use `kafka/Consumer.java` to test the simple process of IoT data.

3. You can also choose to use `flink/Processor.java` to process the IoT data by Flink, eventually store the data to Cassandra and Elastic.

* Kafka 0.10

	* Maven dependency
	
	```
          <dependency>
            <groupId>org.apache.kafka</groupId>
            <artifactId>kafka_2.11</artifactId>
            <version>0.10.2.0</version>
        </dependency>  
        
        <dependency>
            <groupId>org.apache.flink</groupId>
            <artifactId>flink-connector-kafka-0.10_2.11</artifactId>
            <version>1.2.0</version>
        </dependency>
	```
	
	* Example Java code
	
	```java
        DataStream<String> stream = env.addSource(
                new FlinkKafkaConsumer10<>("test", new SimpleStringSchema(), properties));
        return stream;
	```
 
* Flink & Cassandra
 
 	* Maven dependency
	
	```
        <dependency>
            <groupId>com.datastax.cassandra</groupId>
            <artifactId>cassandra-driver-core</artifactId>
            <version>3.1.2</version>
        </dependency>

        <dependency>
            <groupId>org.apache.flink</groupId>
            <artifactId>flink-connector-cassandra_2.11</artifactId>
            <version>1.2.0</version>
        </dependency>
	```
	 
* Flink & Elasticsearch Connector
 
	* Maven dependency
	
	```
        <dependency>
            <groupId>org.apache.flink</groupId>
            <artifactId>flink-connector-elasticsearch2_2.11</artifactId>
            <version>1.2.0</version>
        </dependency>
	```
	
