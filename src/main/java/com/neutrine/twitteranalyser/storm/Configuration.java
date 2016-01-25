package com.neutrine.twitteranalyser.storm;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * Created by lpicanco on 19/01/16.
 */
public class Configuration {

    private static final String CONFIG_FILE = "application.properties";

    private static Configuration instance;

    private Properties config;

    private Configuration() throws IOException {
        File file = new File(CONFIG_FILE);

        InputStream is;

        if (file.exists()) {
            is = new FileInputStream(file);
        } else {
            is = getClass().getResourceAsStream("/" + CONFIG_FILE);
        }

        config = new Properties();
        config.load(is);
    }

    public static Configuration getInstance() throws IOException {
        if (instance == null) {
            instance = new Configuration();
        }

        return instance;
    }

    public String getZookeeperConnect() {
        return config.getProperty("zookeeper.connect");
    }

    public String getKafkaTwitterWordTopic() {
        return config.getProperty("kafka.topic");
    }
}
