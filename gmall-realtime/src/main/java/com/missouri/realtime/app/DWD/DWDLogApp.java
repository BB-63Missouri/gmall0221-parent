package com.missouri.realtime.app.DWD;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONAware;
import com.alibaba.fastjson.JSONObject;
import com.missouri.realtime.app.BaseApp;
import com.missouri.realtime.common.Constant;
import com.missouri.realtime.util.MyCommonUtil;
import com.missouri.realtime.util.MykafkaUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.time.Duration;
import java.util.*;



/**
 * @author Missouri
 * @date 2021/7/28 8:47
 */
/*
作用：对日志数据进行分流，存入DWD层kafka
一.对新老用户进行确认
1.考虑数据的乱序, 使用event-time语义
2.按照mid分组
3.添加5s的滚动窗口
4.使用状态记录首次访问的时间戳
5.如果状态为空, 则此窗口内的最小时间戳的事件为首次访问, 其他均为非首次访问
6.如果状态不为空, 则此窗口内所有的事件均为非首次访问
二.对ods层的数据放入不同的流
    启动日志:主流
    曝光日志：侧流
    页面日志：侧输出流--需要环境即上下文
    需要用侧输出流分流，先找到日志类型分类的属性
三.把数据写入kafka
 */
    //测试：需要开ods_log、dwd_start、dwd_page、dwd_displays三个消费者，没有ods_log消费数据，下面的就没有了
public class DWDLogApp extends BaseApp {
    final String START = "start";
    final String PAGE = "page";
    final String DISPLAYS = "displays";
    final String TS = "ts";
    final String PAGE_ID = "page_id";
    final String COMMON = "common";

    public static void main(String[] args){
        new DWDLogApp().init(
                1,
                "DWDLogApp",
                Constant.TOPIC_ODS_LOG,
                "DWDLogApp",
                20000);
    }

    //里面实现业务，但事实上是调用个业务的方法
    @Override
    protected void run(StreamExecutionEnvironment env, DataStreamSource<String> dataStream) {
        //对新老用户进行确认
        SingleOutputStreamOperator<JSONObject> distinguishedStream = distinguishNewOrOld(dataStream);
        distinguishedStream.print();
        //对ods层的数据放入不同的流
        Map<String, DataStream<JSONObject>> threeStream = splitStream(distinguishedStream);
        //传到kafka
        sendToKafka(threeStream);
    }

    private void sendToKafka(Map<String, DataStream<JSONObject>> threeStream) {
        threeStream
                .get(START)
                //JSONAware有个toJsonString方法
                .map(JSONAware::toJSONString)
                .addSink(MykafkaUtil.getKafkaSink(Constant.TOPIC_DWD_START));
        threeStream
                .get(PAGE)
                .map(JSONAware::toJSONString)
                .addSink(MykafkaUtil.getKafkaSink(Constant.TOPIC_DWD_PAGE));
        threeStream
                .get(DISPLAYS)
                .map(JSONAware::toJSONString)
                .addSink(MykafkaUtil.getKafkaSink(Constant.TOPIC_DWD_DISPLAYS));
    }

    private Map<String, DataStream<JSONObject>> splitStream(SingleOutputStreamOperator<JSONObject> distinguishedStream) {
        //一定要加{}以及类型，不然报错：The types of the interface org.apache.flink.util.OutputTag could not be inferred.
        // Support for synthetic interfaces, lambdas, and generic or raw types is limited at this point
        OutputTag<JSONObject> pageTag = new OutputTag<JSONObject>(PAGE){};
        OutputTag<JSONObject> displaysTag = new OutputTag<JSONObject>(DISPLAYS){};

        SingleOutputStreamOperator<JSONObject> startStream = distinguishedStream

                .process(new ProcessFunction<JSONObject, JSONObject>() {

                    @Override
                    public void processElement(JSONObject value, Context ctx, Collector<JSONObject> out) throws Exception {
                        JSONObject start = value.getJSONObject(START);
                        if (start != null) {
                            out.collect(value);
                        }
                        //剩下的页面有可能同时是曝光日志和页面日志
                        else {
                            JSONObject pages = value.getJSONObject(PAGE);
                            //怎吗遍历操作JSONObject
                            if (pages != null) {
                                ctx.output(pageTag, value);
                            }
                            //displays是数组，注意数据
                            JSONArray displays = value.getJSONArray(DISPLAYS);
                            if (displays != null) {
                                //将一个displays分为单个
                                for (int i = 0; i < displays.size(); i++) {
                                    JSONObject displaysJSONObject = displays.getJSONObject(i);
                                    //补充数据，时间戳和page_id,common数据
                                    displaysJSONObject.put(TS, value.getLong(TS));
                                    displaysJSONObject.put(PAGE_ID, value.getJSONObject(PAGE).getString(PAGE_ID));
                                    displaysJSONObject.putAll(value.getJSONObject(COMMON));
                                    ctx.output(displaysTag, displaysJSONObject);
                                }
                            }
                        }

                    }
                });
        DataStream<JSONObject> pageStream = startStream.getSideOutput(pageTag);
        DataStream<JSONObject> displaysStream = startStream.getSideOutput(displaysTag);

        //收集三个流传到kafka，但这个HashMap的创造。。。
        HashMap<String,DataStream<JSONObject>> result = new HashMap<>();
        result.put(START,startStream);
        result.put(PAGE,pageStream);
        result.put(DISPLAYS,displaysStream);
        return result;
    }

    private SingleOutputStreamOperator<JSONObject> distinguishNewOrOld(DataStreamSource<String> dataStream) {
        //如何识别新老用户，已知有数据流，首先要利用状态，如果状态为空则为新用户，但是考虑到数据的乱序问题，则要使用事件时间，然后利用窗口，
        //窗口中有多个状态，但时间戳最小的是来的最早的，状态如果为空则一定是新用户
        //不能再后面打印，打印只作测试，不然就是...sink类了，不是output类
        return dataStream
                .map(JSON::parseObject)
                .assignTimestampsAndWatermarks(
                    WatermarkStrategy
                            .<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                        .withTimestampAssigner((element, recordTimestamp) -> element.getLong("ts"))
                )
                //common在数据中带一个大括号，表示一个对象
                .keyBy(obj->obj.getJSONObject("common").getString("mid"))
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                .process(new ProcessWindowFunction<JSONObject, JSONObject, String, TimeWindow>() {
                    //通过状态判断
                    private ValueState<Long> timeState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        //给状态付初始值
                        timeState = getRuntimeContext().getState(new ValueStateDescriptor<Long>("timeState", Long.class));
                    }

                    @Override
                    public void process(String s,
                                        Context context,
                                        Iterable<JSONObject> elements,
                                        Collector<JSONObject> out) throws Exception {
                        //如果状态为空，则为某个mid的第一个窗口，找最小时间戳
                        if (timeState.value() == null){
                            List<JSONObject> tolist = MyCommonUtil.tolist(elements);
                            //把时间戳最新的选出来
                            tolist.sort((o1, o2) -> o1.getLong("ts").compareTo(o2.getLong("ts")));
                            tolist.sort(Comparator.comparing(o -> o.getLong("ts")));
                            for (int i = 0; i < tolist.size(); i++) {
                                //common一定返回，不在if中
                                JSONObject common = tolist.get(i).getJSONObject("common");
                                if ( i == 0){
                                    common.put("is_new","1");
                                    //修改之后更新状态，表示不是新用户了
                                    timeState.update(tolist.get(i).getLong("ts"));
                                }else {
                                    common.put("is_new","0");
                                }
                                out.collect(tolist.get(i));
                            }
                        }else {
                            for (JSONObject element : elements) {
                                element.getJSONObject("common").put("is_new","0");
                            }
                        }
                    }
                })
                ;



    }
}
