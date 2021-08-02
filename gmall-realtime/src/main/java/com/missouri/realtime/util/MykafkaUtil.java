package com.missouri.realtime.util;

import com.alibaba.fastjson.JSONObject;
import com.missouri.realtime.bean.TableProcess;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;
import java.nio.charset.StandardCharsets;
import java.util.Properties;

/**
 * @author Missouri
 * @date 2021/7/27 20:08
 */
public class MykafkaUtil {
    //写一个方法返回kafka消费者
    public static FlinkKafkaConsumer<String> getKafkaSource(String groupId,String topic){
        Properties props = new Properties();
        props.setProperty("bootstrap.servers","hadoop162:9092,hadoop163:9092,hadoop164:9092");
        props.setProperty("group.id",groupId);
        //接着上次消费
        props.setProperty("auto.offset.reset","latest");
        props.setProperty("isolation.level","read_committed");
        return new FlinkKafkaConsumer<String>(topic,new SimpleStringSchema(),props);
    }
    //产生生产者的方法
    public static FlinkKafkaProducer<String> getKafkaSink(String topic){
        Properties props = new Properties();
        props.setProperty("bootstrap.servers","hadoop162:9092,hadoop163:9092,hadoop164:9092");
        //后面的设计也是变为字符串,设计flink超时时间，默认1h但，kafka是15m冲突
        props.setProperty("transaction.timeout.ms",15 * 60 + "");
        //"transaction.timeout.ms", 15 * 60 + "" 多一个空，开始前面参数名没写对
        //"transaction.timeout.ms",15 * 60 + " "
        return new FlinkKafkaProducer<String>(
                "default",
                //得注意听
                new KafkaSerializationSchema<String>() {
                    @Override
                    public ProducerRecord<byte[], byte[]> serialize(String element, @Nullable Long timestamp) {
                        return new ProducerRecord<byte[], byte[]>(topic,null,element.getBytes(StandardCharsets.UTF_8) );
                    }
                },
                props,
                //精确一次
                FlinkKafkaProducer.Semantic.EXACTLY_ONCE
        );
    }
    //表名为主题，多个主题的kafka生产者
    public static FlinkKafkaProducer<Tuple2<JSONObject, TableProcess>> getKafkaSink(){
        Properties conf = new Properties();
        conf.setProperty("bootstrap.servers","hadoop162:9092,hadoop163:9092,hadoop164:9092");
        conf.setProperty("transaction.timeout.ms", 15 * 60 * 1000+ "");
        return new FlinkKafkaProducer<Tuple2<JSONObject, TableProcess>>(
            "defult",
                 new KafkaSerializationSchema<Tuple2<JSONObject, TableProcess>>() {
                    @Override
                    public ProducerRecord<byte[], byte[]> serialize(Tuple2<JSONObject, TableProcess> element, @Nullable Long timestamp) {
                          return new ProducerRecord<>(
                                element.f1.getSink_table(),
                                null,
                                element.f0.toJSONString().getBytes(StandardCharsets.UTF_8));
                    }
                },
                conf,
                FlinkKafkaProducer.Semantic.EXACTLY_ONCE
        );
    }
}
