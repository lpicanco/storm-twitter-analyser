package com.neutrine.twitteranalyser.storm.bolt;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import org.apache.storm.shade.org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * Created by lpicanco on 1/20/16.
 */
public class WordFilterBolt extends BaseRichBolt {
    private final Logger log = LoggerFactory.getLogger(this.getClass());

    private OutputCollector outputCollector;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.outputCollector = outputCollector;
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("word"));
    }

    @Override
    public void execute(Tuple tuple) {
        String text = tuple.getString(0);

        if (StringUtils.startsWith(text, "@") || StringUtils.trim(text).length() <= 4) {
            log.debug("Filtering: " + text);
        } else {
            outputCollector.emit(new Values(text));
        }

        outputCollector.ack(tuple);
    }
}
