package org.feiware.iot.cassandra;

import com.datastax.driver.core.Session;
import org.apache.commons.io.IOUtils;

import java.io.IOException;
import java.util.Arrays;

/**
 * Created by feiware on 12/30/16.
 */

public class Initialization {

    public static void init(Session session) {

        String file = null;
        try {
            file = IOUtils.toString(KafkaCassandra.class.getResourceAsStream("/c.sql"));
        } catch (IOException e) {
            e.printStackTrace();
        }
        Arrays.stream(file.split(";"))
                .map(s -> s.trim())
                .filter(s -> !s.isEmpty())
                .map(s -> s + ";")
                .forEach((String str) -> session.execute(str));

        System.out.println("------------------KEYSPACE AND TABLES CREATED-----------------------------");
    }
}
