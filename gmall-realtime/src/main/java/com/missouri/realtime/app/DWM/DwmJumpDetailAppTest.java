package com.missouri.realtime.app.DWM;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.missouri.realtime.app.BaseApp;
import com.missouri.realtime.common.Constant;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
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
 * @date 2021/8/2 14:49
 */
//测试版，
public class DwmJumpDetailAppTest extends BaseApp {
    //初始化
    public static void main(String[] args) {
        new DwmJumpDetailAppTest().init(
                1,"DwmJumpDetail", Constant.TOPIC_DWD_PAGE,"DwmJumpDetail",31000);
    }
    @Override
    protected void run(StreamExecutionEnvironment env, DataStreamSource<String> sourceStream) {
        sourceStream =
                env.fromElements(
                        "{\"common\":{\"mid\":\"101\"},\"page\":{\"page_id\":\"home\"},\"ts\":10000} ",
                        "{\"common\":{\"mid\":\"101\"},\"page\":{\"page_id\":\"home\"},\"ts\":11000} ",
                        "{\"common\":{\"mid\":\"102\"},\"page\":{\"page_id\":\"home\"},\"ts\":10000}",
                        "{\"common\":{\"mid\":\"102\"},\"page\":{\"page_id\":\"good_list\",\"last_page_id\":" +
                                "\"home\"},\"ts\":12000} ",
                        "{\"common\":{\"mid\":\"102\"},\"page\":{\"page_id\":\"good_list\",\"last_page_id\":" +
                                "\"detail\"},\"ts\":50000} "
                );
        //1有个流,然后计算，真正的可以从kafka获取数据
        KeyedStream<JSONObject, String> stream = sourceStream
                .map(new MapFunction<String, JSONObject>() {
                    @Override
                    public JSONObject map(String value) throws Exception {
                        return JSON.parseObject(value);
                    }
                })
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                                .withTimestampAssigner((element, recordTimestamp) -> element.getLong("ts"))
                )
                //用common的mid分组
                .keyBy(obj -> obj.getJSONObject("common").getString("mid"));
        //2用CEP处理，先定义模式  入口-》页面 正常
        //  先用pattern调用方法。began，注意在前面加泛型，where-next-within(超时时间设置)，可以多个条件
        Pattern<JSONObject, JSONObject> pattern = Pattern
                .<JSONObject>begin("start")
                //返回true和true的匹配选择的选择
                .where(new SimpleCondition<JSONObject>() {
                    @Override
                    public boolean filter(JSONObject value) throws Exception {
                        String lastPageId = value.getJSONObject("page").getString("last_page_id");
                        //不存在或者长度是0
                        return lastPageId == null || lastPageId.isEmpty();
                    }
                })
                .next("nextPage")
                .where(new SimpleCondition<JSONObject>() {
                    @Override
                    public boolean filter(JSONObject value) throws Exception {
                        JSONObject page = value.getJSONObject("page");
                        String pageId = page.getString("page_id");
                        String lastPageId = page.getString("last_page_id");
                        //不理解此判断条件，前面有页，自身不为空，此页长度不为0
                        return pageId != null && lastPageId != null && !lastPageId.isEmpty();
                    }
                })
                //超时时间设定
                .within(Time.seconds(5));
        //3.把模式应用到流上  CEP调用pattern方法，将流和模式匹配传进去
        PatternStream<JSONObject> ps = CEP.pattern(stream, pattern);

        //4.用select来取出满足模式的数据，或者超时时间
        SingleOutputStreamOperator<JSONObject> normal = ps
                .select(
                        new OutputTag<JSONObject>("timeOut") {
                        },
                        //超时数据，跳出明细
                        new PatternTimeoutFunction<JSONObject, JSONObject>() {
                            @Override
                            public JSONObject timeout(Map<String, List<JSONObject>> pattern,
                                                      long timeoutTimestamp) throws Exception {
                                //.get(0)没有次数，所以一个 ？？？
                                return pattern.get("start").get(0);
                            }
                        },
                        //正常的数据，不用管
                        new PatternSelectFunction<JSONObject, JSONObject>() {
                            @Override
                            public JSONObject select(Map<String, List<JSONObject>> pattern) throws Exception {
                                return null;
                            }
                        }
                );
        //跳出数据放到侧输出流
        normal.getSideOutput(new OutputTag<JSONObject>("timeOut"){}).print("jump");
    }
}
