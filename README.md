# Amazon Kinesis Data Analytics for Java - Leveraging the Apache Flink Table Api
This sample project demonstrates how to leverage Kinesis Data Analytics for Java to ingest multiple streams of JSON data, catalog those streams as temporal tables using the Apache Flink Table API and build analytical application which joins these data sets together.

## Use case overview
In this sample project you will leveraging the Apache Flink Table API to catalog two seperate Kinesis streams.  One stream will simulate orders at a rate of 100 records per second.  These orders have an order amount and a currency.  The second stream contains the exchange rates for the currency where the exchange rates are updated every 5 seconds.  The application will join the two data sets together and return both the original order amount along with the amount which has been converted by the exchange rate which was applicable at the time of the transaction.  

## Sample-Code Build and Deploy
The following instructions mimic the AWS documentation for building and deploying other sample projects, but have a few modifications specific to this project. These instructions assume you have completed the necessary pre-requisites before you begin.  The pre-requisites will guide you through the process of installing the Java SDK, a Java IDE, Git, and Maven.
```
https://docs.aws.amazon.com/kinesisanalytics/latest/java/getting-started.html#setting-up-prerequisites
```

### Add the Apache Flink Kinesis connector to your local environment
The Apache Flink Kinesis Connector has a dependency on code licensed under the [Amazon Software License](https://aws.amazon.com/asl/) (ASL) so is not available in the Maven central repository.  To download and compile the connector, perform the following steps.
* From your GitHub root folder, execute the following command to download the Apache Flink source code. 
```
git clone https://github.com/apache/flink
```
* Navigate to the newly created *flink* directory and execute the following command to build and install the connector.
```
mvn clean install -Pinclude-kinesis -DskipTests -pl flink-connectors/flink-connector-kinesis
```

### Create the Data Streams
Create 3 Amazon Kinesis Data Streams.  2 streams will be used for data inputs and 1 will be used for data outputs. The following script can be run from the command-line interface.

```
$ aws kinesis create-stream \
--stream-name flinkjoin-order \
--shard-count 1 \
--region us-west-2 \

$ aws kinesis create-stream \
--stream-name flinkjoin-exchangerate \
--shard-count 1 \
--region us-west-2 \

$ aws kinesis create-stream \
--stream-name flinkjoin-output \
--shard-count 1 \
--region us-west-2 \

```

### Generate the Sample Data
Leverage the Kinesis Data Generator (https://awslabs.github.io/amazon-kinesis-data-generator/web/help.html) application to simulate traffic into the 2 input data streams.  

* For the order stream, configure the following template at a rate of 100 records per second.
```
{
    "id": {{random.number(1000000000)}},
    "orderTime": "{{date.utc}}",
    "amount":{{random.number({
            "min":100,
            "max":1000
        })}},
    "currency":"{{random.arrayElement(["EUR","USD","GBP", "AUD", "CAD"])}}"
}
```
* For the exchange rate stream, configure the following template at a rate of 1 records per second.
```
{
    "exchangeRateTime":"{{date.utc}}",
    "currency":"{{random.arrayElement(["EUR","USD","GBP", "AUD", "CAD"])}}",
    "rate":{{random.number({
            "min":2,
            "max":100
        })}}
}
```

### Download and package the application
* From your GitHub root folder, execute the following command to download the Apache Flink source code. 
```
git clone https://github.com/aws-samples/amazon-kinesis-data-analytics-flinktableapi
```
* Navigate to the newly created *amazon-kinesis-data-analytics-flinktableapi* directory containing the pom.xml file.  Execute the following command to create your JAR file.
```
mvn package
```
* If the application compiles successfully, the following file is created.  
```
target/aws-kinesis-analytics-java-apps-1.0.jar
```
* At this point, proceed with the getting started guide to upload and start your application 
```
https://docs.aws.amazon.com/kinesisanalytics/latest/java/get-started-exercise.html#get-started-exercise-6
```

### Development Environment Setup
You can inspect and modify the code by modifying the .java files located within the project tree.  In my development, I used IntelliJ IDEA from Jetbrains. I followed the steps listed within the Apache Flink Documentation to setup my environment.
```
https://ci.apache.org/projects/flink/flink-docs-stable/flinkDev/ide_setup.html
```
Once the cloned project is imported as a Maven project, to be able to run and debug the application within the IDE, you must conduct an additional step of settting the project runtime configuration.  Add a configuration using the *Application* template and set the following parameters
1. *Main class* - com.amazonaws.services.kinesisanalytics.StreamingJob
1. *Working directory* - $MODULE_WORKING_DIR$
1. *Use classpath of module* - aws-kinesis-analytics-java-apps
1. *JRE* - Default (1.8 - SDK of 'aws-kinesis-analytics-java-apps' module)

## Code Walkthrough
The following section breaks down the code and explains what is ocurring at each step in the Application.

### JSON Stream Definition
Import the Kinesis Stream into the application by leveraging the FlinkKinesisConsumer class within the Apache Flink Kinesis Connector.

1. Initialize the StreamExecutionEnvironment and set the StreamTimeCharacteristic. By setting this you are indicating that time processing should be based on a field which will be extracted from the data as opposed to being derived by when the message is processed by the application. 
```
final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
```
2. Set the REGION of the streams and the starting point for processing.  These values will be stored into an Properties object. 
```
Properties inputProperties = new Properties();
inputProperties.setProperty(ConsumerConfigConstants.AWS_REGION, "us-west-2");
inputProperties.setProperty(ConsumerConfigConstants.STREAM_INITIAL_POSITION, "LATEST");
```
3. Pass the input properties into the constructor of a new FlinkKinesisConsumer object to catalog the DataStream within the StreamExecutionEnvironment.  Notice that a JsonNodeDeserializationSchema object is used to convert the incoming JSON to *generic* ObjectNode.  
```
DataStream<ObjectNode> orderStreamObject =  env.addSource(
        new FlinkKinesisConsumer<>(
                "flinkjoin-order", 
                new JsonNodeDeserializationSchema(), 
                inputProperties));

DataStream<ObjectNode> exchangeRateStreamObject =  env.addSource(
        new FlinkKinesisConsumer<>(
                "flinkjoin-exchangerate", 
                new JsonNodeDeserializationSchema(), 
                inputProperties));
```
        
### JSON Stream Parsing and Time Attribute Definition
Map incomming data from the generic Object Node to a POJO (Plain Old Java Object) for the application to know the structure and types of the data. 

1. Define a class with attribute names which match the name/value pair names in the JSON messages for each stream.
```
public class Order {
    public int id;
    public Timestamp orderTime;
    public int amount;
    public String currency;
}

public class ExchangeRate {
    public Timestamp exchangeRateTime;
    public String currency;
    public int rate;
}
```
2. Execute a map function for every entry in the generic Object Node stream which calls the ObjectMapper for each stream.  The ObjectMapper will parse the JSON fields and construct the corresponding POJO object.
```
DataStream<Order> orderStream = orderStreamObject
        .map((ObjectNode object) -> {
            //For debugging input
            //System.out.println(object.toString());
            ObjectMapper mapper = new ObjectMapper();
            Order order = mapper.readValue(object.toString(), Order.class);
            return order;
        });

DataStream<ExchangeRate> exchangeRateStream = exchangeRateStreamObject
        .map((ObjectNode object) -> {
            //For debugging input
            //System.out.println(object.toString());
            ObjectMapper mapper = new ObjectMapper();
            ExchangeRate exchangeRate = mapper.readValue(object.toString(), ExchangeRate.class);
            return exchangeRate;
        });
```
3. Execute the assignTimestampAndWatermarks function to assign the *Time Attribute* field (rowtime) to a field within the object.
```
DataStream<Order> orderStreamWithTime = orderStream
        .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<Order>() {
            @Override
            public long extractAscendingTimestamp(Order element) {
                    return element.orderTime.getTime();
                }});

DataStream<ExchangeRate> exchangeRateStreamWithTime = exchangeRateStream
        .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<ExchangeRate>() {
            @Override
            public long extractAscendingTimestamp(ExchangeRate element) {
                return element.exchangeRateTime.getTime();
            }});                     
```
4. Convert from a *DataStream* to a *Table*, adding the rowtime *Time Attribute* to the table and aliasing it with the name eventtime.  
```
StreamTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(env);
Table orderTable = tableEnv
    .fromDataStream(orderStreamWithTime, "id, orderTime, amount, currency, eventtime.rowtime");
Table exchangeRateTable = tableEnv
    .fromDataStream(exchangeRateStreamWithTime, "exchangeRateTime, currency, rate, eventtime.rowtime");
```

### User Defined Function
The Apache Flink Table API currently does not have a function which can convert a Timestamp to a formatted string and as a result will return the Timestamp as a number representing the milliseconds since 1/1/1970 GMT. Define a simple UDF (user-defined function) for the output data to be human readeable.
1. Create a class which extends the *ScalarFunction* class and overrides the *eval* and *getResultType* functions.
```
public class TimestampToString extends ScalarFunction {
    public String eval(Timestamp t) {
        return t.toString();
    }

    public TypeInformation<?> getResultType(Class<?>[] signature) {
        return Types.STRING;
    }
}
```
2. In your code, register the UDF.
```
tableEnv.registerFunction("TimestampToString", new TimestampToString());
```

### Querying the Data
Define a SQL query for the resultSet table.  Join *Orders* to *ExchangeRates* based on the currency.  To ensure the exchange rate corresponding to the order is returned, also define how the Time Attribute field (eventtime) is related.
```
Table resultTable = tableEnv.sqlQuery(""+
        "SELECT o.id, " +
        "  TimestampToString(o.orderTime) orderTime, " +
        "  o.amount originalAmount, " +
        "  (o.amount*r.rate) convertedAmount " +
        "FROM " +
        "  Orders o," +
        "  ExchangeRates r " +
        "WHERE o.currency = r.currency " +
        "  AND o.eventtime >= r.eventtime " +
        "  AND r.eventtime > (o.eventtime - INTERVAL '5' SECOND)"
    );
```

### Output Processing
Export the results of the query into a Kinesis Stream by leveraging the FlinkKinesisProducer class within the Apache Flink Kinesis Connector.

1. Define a class with attribute names which match the name/value pair names of your desired output JSON messages.
```
public class Result {
    public int id;
    public String orderTime ;
    public int originalAmount;
    public int convertedAmount;
}
```
2. Convert the result *Table* into a *DataStream* of objects using the *Result* class you created.
```
DataStream<Result> resultSet = tableEnv.toAppendStream(resultTable,Result.class);
```
3. Set the REGION of the output stream and store it into an Properties object. 
```
Properties outputProperties = new Properties();
outputProperties.setProperty(ConsumerConfigConstants.AWS_REGION, "us-west-2");
```
4. Pass the properties into a a FlinkKinesisProducer sink which will serialize the stream of objects into a stream of Byte[] leveraging the ObjectMapper.   
```
FlinkKinesisProducer<Result> streamSink = new FlinkKinesisProducer<Result>(new SerializationSchema<Result>() {
    @Override
    public byte[] serialize(Result element) {
        ObjectMapper mapper = new ObjectMapper();
        byte[] output;
        try {
            output = mapper.writeValueAsString(element).getBytes();
        } catch (Exception e) {
            output = "".getBytes();
        }
        return output;
    }
}, outputProperties);
streamSink.setDefaultStream("flinkjoin-output");
streamSink.setDefaultPartition("0");
```
5. Set the output destination (Sink) for the resultSet table and execute the application.
```
resultSet.addSink(streamSink);
env.execute("Apache Flink Streaming - Orders w/ Exchange Rates");
```

## License Summary
This sample code is made available under the MIT-0 license. See the LICENSE file.
