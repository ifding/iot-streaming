package org.feiware.iot;


import org.feiware.iot.flink.Processor;
import org.feiware.iot.kafka.Consumer;


/**
 * Created by feiware on 12/28/16.
 */

public class App {

    public static void main(String[] args) throws Exception {

        if (args.length < 1) {
            throw new IllegalArgumentException("Please choose 'consumer' or 'flink' to process the message");
        }
        switch (args[0]) {
            case "consumer":
                Consumer.main(args);
                break;
            case "flink":
                Processor.main(args);
                break;
            default:
                throw new IllegalArgumentException("Sorry, don't know how to do " + args[0]);
        }
    }
}
