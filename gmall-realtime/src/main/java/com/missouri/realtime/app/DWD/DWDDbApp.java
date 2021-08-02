package com.missouri.realtime.app.DWD;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.missouri.realtime.app.BaseApp;
import com.missouri.realtime.bean.TableProcess;
import com.missouri.realtime.common.Constant;
import com.missouri.realtime.util.HbaseUtil;
import com.missouri.realtime.util.MykafkaUtil;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import java.util.Arrays;
import java.util.List;


/**
 * @author MissouriF
 * @date 2021/7/29 14:31
 */
/*
    错误1:NOT ENFORCED 要加在 PRIMARY KEY 后面，flink目前还不支持这种模式
 */
//日志数据分流，分两个流，维度表流借助phoenix动态传入Hbase,事务表流流回kafka
public class DWDDbApp extends BaseApp {
    public static void main(String[] args) {
        new DWDDbApp().init(
                1,
                "dwd_db",
                Constant.TOPIC_ODS_DB,
                "dwd_db",
                22000
        );
    }
    //具体逻辑
    @Override
    protected void run(StreamExecutionEnvironment env, DataStreamSource<String> dataStreamSource) {
        //首先对获得的流处理，简单的etl
        SingleOutputStreamOperator<JSONObject> etlDataStream = EtlDataStream(dataStreamSource);
        //获取表中的数据另起一个配置流，并且过滤
        SingleOutputStreamOperator<TableProcess> readTableProcessStream = readTableProcess(env);
        //主流和配置流connect
        SingleOutputStreamOperator<Tuple2<JSONObject, TableProcess>> connectedStreams = connectStrem(etlDataStream, readTableProcessStream);
        //根据配置，实现动态分流
        Tuple2<SingleOutputStreamOperator<Tuple2<JSONObject, TableProcess>>, DataStream<Tuple2<JSONObject, TableProcess>>> kafkaHbaseStream = dynamicSplitStream(connectedStreams);
        //传到kafka
        sendToKafka(kafkaHbaseStream.f0);
        //传到hbase，借助phoenixjdbc,自动创建topic，默认分区按kafka配置
        sendToHbase(kafkaHbaseStream.f1);

    }
/*
1、向hbase(phoenix)创建表的时候，表不会自动创建
2、写入数据
 */
    private void sendToHbase(DataStream<Tuple2<JSONObject, TableProcess>> stream) {
        stream
                .keyBy(value -> value.f1.getSink_table())
                .addSink(HbaseUtil.getPhoenixSink());
    }

    private void sendToKafka(SingleOutputStreamOperator<Tuple2<JSONObject, TableProcess>> kafkaStream) {
        kafkaStream.addSink(MykafkaUtil.getKafkaSink());
    }

    //分流，通过判断sink_type类型
    private Tuple2<SingleOutputStreamOperator<Tuple2<JSONObject, TableProcess>>, DataStream<Tuple2<JSONObject, TableProcess>>> dynamicSplitStream(SingleOutputStreamOperator<Tuple2<JSONObject, TableProcess>> connectedStreams) {
        //创建侧输出流的tag，侧输出流要弄成匿名内部类形式
        OutputTag<Tuple2<JSONObject, TableProcess>> hbaseTag = new OutputTag<Tuple2<JSONObject, TableProcess>>("hbase"){};

        //通过pt里面的sink_type类进行分流
        SingleOutputStreamOperator<Tuple2<JSONObject, TableProcess>> kafkaStream = connectedStreams.process(new ProcessFunction<Tuple2<JSONObject, TableProcess>, Tuple2<JSONObject, TableProcess>>() {
            @Override
            public void processElement(
                    Tuple2<JSONObject, TableProcess> value,
                    Context ctx,
                    Collector<Tuple2<JSONObject, TableProcess>> out) throws Exception {

                //因为只需要data的数据，筛选返回的数据,key只要value.f0.getJSONObject("data")
                Tuple2<JSONObject, TableProcess> data = Tuple2.of(value.f0.getJSONObject("data"), value.f1);
                //过滤列
                filterColumns(data);

                if (TableProcess.SINK_TYPE_KAFKA.equals(value.f1.getSink_type())) {
                    out.collect(data);
                } else if (TableProcess.SINK_TYPE_HBASE.equals(value.f1.getSink_type())) {
                    ctx.output(hbaseTag, data);
                }
            }
            //过滤掉多余的列，根据数据要求
            private void filterColumns(Tuple2<JSONObject, TableProcess> data){
                JSONObject jsonObject = data.f0;
                //list只能读不能删 删除columns里面没有的kv，columns是主要数据 对着data数据的k和columns里面比
                List<String> columns = Arrays.asList(data.f1.getSink_columns().split(","));
                //jsonObject是一个map，能迭代删除，如果key中没有columns数据则可以删
                jsonObject.keySet().removeIf(key -> !columns.contains(key));
            }
        });
        DataStream<Tuple2<JSONObject, TableProcess>> hbaseStram = kafkaStream.getSideOutput(hbaseTag);
        return Tuple2.of(kafkaStream,hbaseStram);
    }

    //两个流连接，做好分流准备
    private SingleOutputStreamOperator<Tuple2<JSONObject, TableProcess>> connectStrem(SingleOutputStreamOperator<JSONObject> etlDataStream, SingleOutputStreamOperator<TableProcess> readTableProcessStream) {
        /*
            动态分流
                目标: 应该得到一个新的流, 新的流存储的数据类型应该是一个二维元组
                <JSONObject, TableProcess>

            碰到一条数据流中的数据, 找一个TableProcess
            key: source_table:operate_type
            value: TableProcess
         */
        MapStateDescriptor<String, TableProcess> tpStateDesc = new MapStateDescriptor<>("tpState", String.class, TableProcess.class);
        //先把readProcess做成广播流，跟下面所有的主流connect
        BroadcastStream<TableProcess> tpBroadcastStream = readTableProcessStream.broadcast(tpStateDesc);
        //广播connect
        return etlDataStream
                .keyBy(obj-> obj.get("table"))
                .connect(tpBroadcastStream)
                .process(new KeyedBroadcastProcessFunction<String, JSONObject, TableProcess, Tuple2<JSONObject,TableProcess>>() {
                    @Override
                    public void processElement(JSONObject input,
                                               ReadOnlyContext ctx,
                                               Collector<Tuple2<JSONObject, TableProcess>> out) throws Exception {
                        ReadOnlyBroadcastState<String, TableProcess> tpState = ctx.getBroadcastState(tpStateDesc);
                        String key = input.getString("table") + ":"
                                + input.getString("type").replaceAll("bootstrap-", "");
                        //因为在广播状态中存的就是上面key形式的key，所以直接获取
                        TableProcess tp = tpState.get(key);
                        //tp就是数据
                        if(tp != null){
                            out.collect(Tuple2.of(input,tp));
                        }
                    }

                    @Override
                    public void processBroadcastElement(TableProcess tp,
                                                        Context ctx,
                                                        Collector<Tuple2<JSONObject, TableProcess>> out) throws Exception {
                        //把每条来的数据放入状态
                        BroadcastState<String, TableProcess> broadcastState = ctx.getBroadcastState(tpStateDesc);
                        String key = tp.getSource_table() + ":" + tp.getOperate_type();
                        //状态中存放的key为表和类型，value为TableProcess
                        broadcastState.put(key,tp);
                    }
                });
    }
    
    //制作一个配置流，作广播流
    private SingleOutputStreamOperator<TableProcess> readTableProcess(StreamExecutionEnvironment env) {
        //1.用sql查询全部数据建立一个映射表
        //2、监控mysql的更新变化

        StreamTableEnvironment tenv = StreamTableEnvironment.create(env);
        tenv
                .executeSql("CREATE TABLE `table_process` (\n" +
                        "  `source_table` string ,\n" +
                        "  `operate_type` string ,\n" +
                        "  `sink_type` string ,\n" +
                        "  `sink_table` string ,\n" +
                        "  `sink_columns` string ,\n" +
                        "  `sink_pk` string ,\n" +
                        "  `sink_extend` string ,\n" +
                        "  PRIMARY KEY (`source_table`,`operate_type`) NOT ENFORCED \n" +
                        ")with(" +
                        "   'connector' = 'mysql-cdc' ," +
                        "   'hostname' = 'hadoop162' ," +
                        "   'port' = '3306' ," +
                        "   'username' = 'root' ," +
                        "   'password' = 'aaaaaa' ," +
                        "   'database-name' = 'gmall2021_realtime' ," +
                        "   'table-name' = 'table_process' ," +
                        "   'debezium.snapshot.mode' = 'initial' " +
                        ")");
        Table table = tenv.from("table_process");

        return tenv
                //??返回流？？
                .toRetractStream(table, TableProcess.class)
                //过滤数据，有则返回ture，则保留输入的数
                .filter(t->t.f0)
                //剩下数据只要value的数据
                .map(t->t.f1);
    }
    
    //轻度清洗数据，过滤坏数据
    private SingleOutputStreamOperator<JSONObject> EtlDataStream(DataStreamSource<String> dataStreamSource) {
        return
        dataStreamSource
                //转为obj
                .map(JSON::parseObject)
                .filter(obj ->
                        obj
                                .getString("database") != null
                        && obj.getString("table") != null
                        && obj.getString("type") != null
                        && obj.getString("type").contains("insert")
                        && obj.getString("data") != null
                        && obj.getString("data").length() > 10
                        );

    }
}
