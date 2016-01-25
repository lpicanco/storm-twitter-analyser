package com.neutrine.twitteranalyser.storm;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.StormTopology;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import com.neutrine.twitteranalyser.storm.bolt.MongoPersistBolt;
import com.neutrine.twitteranalyser.storm.bolt.WordCounterBolt;
import com.neutrine.twitteranalyser.storm.bolt.WordFilterBolt;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.kafka.KafkaSpout;
import storm.kafka.SpoutConfig;
import storm.kafka.StringScheme;
import storm.kafka.ZkHosts;

import java.io.IOException;

/**
 * Created by lpicanco on 1/20/16.
 */
public class WordCountTopology {

    private final Logger log = LoggerFactory.getLogger(this.getClass());

    public static void main(String[] args) throws Exception {
        StormSubmitter.submitTopology("word-count-analysis", createConfig(), createTopology());
        //LocalCluster cluster = new LocalCluster();
        //cluster.submitTopology("word-count-analysis", createConfig(), createTopology());

        //Thread.sleep(10000);
        //cluster.shutdown();

        System.exit(0);
    }

    private static StormTopology createTopology() throws IOException {
        SpoutConfig kafkaConf = new SpoutConfig(new ZkHosts("taurus.lan.luizpicanco.com:2181"),
                Configuration.getInstance().getKafkaTwitterWordTopic(), "/kafka", "KafkaSpoutTwitterWord");


        kafkaConf.scheme = new SchemeAsMultiScheme(new StringScheme());
        TopologyBuilder topology = new TopologyBuilder();

        topology.setSpout("kafka_spout", new KafkaSpout(kafkaConf), 1);

        topology.setBolt("word_filter", new WordFilterBolt(), 1)
                .shuffleGrouping("kafka_spout");

        topology.setBolt("word_counter", new WordCounterBolt(), 2)
                .fieldsGrouping("word_filter", new Fields("word"));

        topology.setBolt("mongo_persist", new MongoPersistBolt(), 1)
                .shuffleGrouping("word_counter");

        return topology.createTopology();
    }

    private static Config createConfig() {
        Config conf = new Config();
        conf.setDebug(true);
        conf.setNumWorkers(1);
        return conf;
    }
}