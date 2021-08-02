package com.missouri.realtime.app;

import com.missouri.realtime.util.MykafkaUtil;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.*;

/**
 * @author Missouri
 * @date 2021/8/2 19:58
 */
//可以完全替换原始版本的基类
public abstract class BasesApp {
    public void init(
            int defultParallelism,
            String groupId,
            String ck,
            Integer port,
            String topic,
            String... topics
    ){
        System.setProperty("HADOOP_USER_NAME","atguigu");
        Configuration conf = new Configuration();
        conf.setInteger("rest.port",port);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(defultParallelism);

        //设置CK相关参数  CK:checkpoint
        //1.设置精准一次的checkpoint
        env.enableCheckpointing(5000, CheckpointingMode.EXACTLY_ONCE);
        //2.checkpoint必须一分钟完成，否则废弃
        env.getCheckpointConfig().setCheckpointTimeout(60*1000);
        //3.开启中断保存
        env
                .getCheckpointConfig()
                .enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        //4.设置状态后端
        env.setStateBackend(new HashMapStateBackend())
                .getCheckpointConfig()
                .setCheckpointStorage("hdfs://hadoop162:8020/gmall2021/flink"+ck);

        //首先将所有topic放在一个集合，再遍历生产流 set集合，如果传重复可以去重
        HashSet<String> set = new HashSet<>(Arrays.asList(topics));//把集合当参数传比后面添加快
        set.add(topic);
        //用map存储方便取用,他人可通过对应主题取流
        Map<String, DataStreamSource<String>> sourceStreams = new HashMap<>();
        for (String s : set) {
            DataStreamSource<String> stream = env
                    .addSource(MykafkaUtil.getKafkaSource(groupId, s));
            sourceStreams.put(s,stream);
        }


        run(env,sourceStreams);

        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    protected abstract void run(StreamExecutionEnvironment env, Map<String, DataStreamSource<String>> sourceStreams);
}
