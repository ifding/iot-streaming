package org.ifding.iot.utils;

/**
 * Created by ifding on 12/28/16.
 */

public class IoTDataConf {

    // producer/consumer topic
    public String TOPIC;

    // speed at which records are produced in milliseconds
    public  int PRODUCTION_SPEED;

    // time for how long the consumer waits for nex data in milliseconds
    public long CONSUMER_TIMEOUT;

    // size of window and slide in seconds
    public int WINDOW;
    public int SLIDE;

    public String CASSANDRA_DNS;

    public String SPARK_MASTER;
    public int SPARK_PORT;

    public String[] ELASTIC_DNS;
    public int ELASTIC_PORT;
    public String ELASTIC_FLUSH;
    public String ELASTIC_CLUSTER;
}
