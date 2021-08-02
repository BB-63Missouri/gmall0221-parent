package com.missouri.realtime.app;

import com.missouri.realtime.util.MykafkaUtil;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author Missouri
 * @date 2021/7/27 20:16
 */
public abstract class BaseApp {

    //设定数据来源进行操作，具体业务
    protected abstract void run(StreamExecutionEnvironment env,
                                DataStreamSource<String> dataStreamSource);

    //抽象类里的非抽象方法，只适用于本类,初始化本类用(静态方法能被继承不能被复写，所以基类需要)
    public void init(
            int defultParallelism,
            String groupId,
            String topic,
            String ck,
            Integer port){

        //设置本用户在hadoop上为管理用户
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

        DataStreamSource<String> sourceStream = env
                .addSource(MykafkaUtil.getKafkaSource(groupId, topic));

        run(env,sourceStream);

        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
