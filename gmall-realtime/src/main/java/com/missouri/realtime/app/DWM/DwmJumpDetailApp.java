package com.missouri.realtime.app.DWM;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONAware;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.JSONWriter;
import com.missouri.realtime.app.BaseApp;
import com.missouri.realtime.common.Constant;
import com.missouri.realtime.util.MykafkaUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.PatternTimeoutFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.OutputTag;

import java.time.Duration;
import java.util.List;
import java.util.Map;

/**
 * @author Missouri
 * @date 2021/8/2 16:42
 */
public class DwmJumpDetailApp extends BaseApp {

    public static void main(String[] args) {
        new DwmJumpDetailApp().init(
                1,
                //随版本升级而改进数字
                "DwmJumpDetailAppV1",
                Constant.TOPIC_DWD_PAGE,
                "DwmJumpDetailAppV1",
                32000
        );
    }

    @Override
    protected void run(StreamExecutionEnvironment env, DataStreamSource<String> sourceStream) {
        //下面为测试
        /*sourceStream =
                env.fromElements(
                        "{\"common\":{\"mid\":\"101\"},\"page\":{\"page_id\":\"home\"},\"ts\":10000} ",
                        "{\"common\":{\"mid\":\"101\"},\"page\":{\"page_id\":\"home\"},\"ts\":11000} ",
                        "{\"common\":{\"mid\":\"102\"},\"page\":{\"page_id\":\"home\"},\"ts\":10000}",
                        "{\"common\":{\"mid\":\"102\"},\"page\":{\"page_id\":\"good_list\",\"last_page_id\":" +
                                "\"home\"},\"ts\":17000} ",
                        "{\"common\":{\"mid\":\"102\"},\"page\":{\"page_id\":\"good_list\",\"last_page_id\":" +
                                "\"detail\"},\"ts\":50000} "
                );*/

        //获得流
        KeyedStream<JSONObject, String> stream = sourceStream
                .map(JSON::parseObject)
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                                /*.withTimestampAssigner(new SerializableTimestampAssigner<JSONObject>() {
                                    @Override
                                    public long extractTimestamp(JSONObject element, long recordTimestamp) {
                                        return element.getLong("ts");
                                    }
                                })*/
                                .withTimestampAssigner((element, recordTimestamp) -> element.getLong("ts"))
                )
                .keyBy(obj -> obj.getJSONObject("common").getString("mid"));
        //定义模式，开始是入口，第二个也是入口，则第一个一定是跳出数据
        Pattern<JSONObject, JSONObject> pattern = Pattern
                .<JSONObject>begin("start")
                .where(new SimpleCondition<JSONObject>() {
                    @Override
                    public boolean filter(JSONObject value) throws Exception {
                        String lastPageId = value.getJSONObject("page").getString("last_page_id");
                        return lastPageId == null || lastPageId.isEmpty();
                    }
                })
                .next("newPage")
                .where(new SimpleCondition<JSONObject>() {
                    @Override
                    public boolean filter(JSONObject value) throws Exception {
                        //判断入口
                        String lastPageId = value.getJSONObject("page").getString("last_page_id");
                        return lastPageId == null || lastPageId.isEmpty();
                    }
                })
                .within(Time.seconds(5));
        //把模式应用到流上
        PatternStream<JSONObject> ps = CEP.pattern(stream, pattern);
        //取出满足的数据
        SingleOutputStreamOperator<JSONObject> operator = ps
                .select(
                        new OutputTag<JSONObject>("outTimeV1") {
                        },
                        new PatternTimeoutFunction<JSONObject, JSONObject>() {
                            @Override
                            public JSONObject timeout(Map<String, List<JSONObject>> pattern, long timeoutTimestamp) throws Exception {
                                return pattern.get("start").get(0);
                            }
                        },
                        new PatternSelectFunction<JSONObject, JSONObject>() {
                            @Override
                            public JSONObject select(Map<String, List<JSONObject>> pattern) throws Exception {
                                return pattern.get("start").get(0);
                            }
                        }

                );
        operator
                .union(operator.getSideOutput(new OutputTag<JSONObject>("outTimeV1"){}))
                .map(JSONAware::toJSONString)
                .addSink(MykafkaUtil.getKafkaSink(Constant.TOPIC_DWM_USER_JUMP_DETAIL));
    }
}
