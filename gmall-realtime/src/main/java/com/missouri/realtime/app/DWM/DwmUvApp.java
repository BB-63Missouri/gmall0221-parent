package com.missouri.realtime.app.DWM;

import com.alibaba.fastjson.JSON;
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
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

/**
 * @author Missouri
 * @date 2021/7/31 16:32
 */
//数据从page页面获取独立访问，则从page-topic主题获取,去重，把第一条记录放进去
public class DwmUvApp extends BaseApp {

    public static void main(String[] args) {
        //获取数据
        new DwmUvApp().init(1,"DwmUvApp", Constant.TOPIC_DWD_PAGE,"DwmUvApp",30000);
    }


    @Override
    protected void run(StreamExecutionEnvironment env, DataStreamSource<String> sourceStream) {
        sourceStream
                .map(JSON::parseObject)
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                                .withTimestampAssigner((element, recordTimestamp) -> element.getLong("ts"))

                )
                //获取common里面的id为主键
                .keyBy(obj->obj.getJSONObject("common").getString("mid"))
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                //去重
                .process(new ProcessWindowFunction<JSONObject, JSONObject, String, TimeWindow>() {
                    //时间格式
                    private SimpleDateFormat simpleDateFormat;
                    private ValueState<Long> uvState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        uvState = getRuntimeContext().getState(new ValueStateDescriptor<Long>("uvState",Long.class));
                        simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
                    }
                    //跨天问题，去重，采用窗口结束时间为当前时间，然后用水印的时间为开始的时间，即旧时间
                    @Override
                    public void process(String s, Context context, Iterable<JSONObject> elements, Collector<JSONObject> out) throws Exception {

                        String now = simpleDateFormat.format(context.window().getEnd());
                        //判空防空指针
                        String old = simpleDateFormat.format(uvState.value() == null ? 0L : uvState.value());

                        if(!now.equals(old)){
                            //即时间跨天
                            uvState.clear();
                        }
                        //firstVisit.value()通常不为空，要么就是第一时间，要么就是跨天上面清空时间
                        if (uvState.value() == null){
                            List<JSONObject> list = MyCommonUtil.tolist(elements);

                            JSONObject min = Collections.min(list, Comparator.comparing(e -> e.getString("ts")));
                            out.collect(min);
                            uvState.update(min.getLong("ts"));
                        }
                    }
                })
                .map(JSONAware::toJSONString)

                .addSink(MykafkaUtil.getKafkaSink(Constant.TOPIC_DWM_UV));
    }
}
