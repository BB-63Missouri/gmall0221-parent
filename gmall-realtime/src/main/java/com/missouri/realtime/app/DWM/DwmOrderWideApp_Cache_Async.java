package com.missouri.realtime.app.DWM;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.missouri.realtime.app.BasesApp;
import com.missouri.realtime.bean.OrderDetail;
import com.missouri.realtime.bean.OrderInfo;
import com.missouri.realtime.bean.OrderWide;
import com.missouri.realtime.common.Constant;
import com.missouri.realtime.function.DimAsyncFunction;
import com.missouri.realtime.util.DimUtil;
import com.missouri.realtime.util.MykafkaUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import redis.clients.jedis.Jedis;


import java.sql.Connection;
import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * @author Missouri
 * @date 2021/8/4 11:38
 */
/*
优化2，使用异步查询
在flink MapFunction中通常情况下单个并行只能用同步的方式去访问，一但查询连接数据量大 则等待时间非常的长
flink在1.2中引入异步IO，不阻塞等待，没有回应就继续下个访问，来回应了就处理，节省访问等待时间

使用异步异步API的先决条件：
   正确地实现数据库（或键/值存储）的异步 I/O 交互需要支持异步请求的数据库客户端。
   没有客户端可以用线程池同步调用的方法，将同步的客户端转为有限的并发客户端，但效率比客户端低

    由于Hbase的phoenix皮肤没有提供异步的客户端

    1.实现分发请求的 AsyncFunction
2.获取数据库交互的结果并发送给 ResultFuture 的 回调函数
3、将异步 I/O 操作应用于 DataStream 作为 DataStream 的一次转换操作。
*/
public class DwmOrderWideApp_Cache_Async extends BasesApp {

    public static void main(String[] args) {
        new DwmOrderWideApp_Cache_Async().init(
                1,
                "DwmOrderWideApp",
                "DwmOrderWideApp",
                20000,
                Constant.TOPIC_DWD_ORDER_INFO,
                Constant.TOPIC_DWD_ORDER_DETAIL
        );

    }
    @Override
    protected void run(StreamExecutionEnvironment env, Map<String, DataStreamSource<String>> sourceStreams) {
        //1.事实表进行join
        SingleOutputStreamOperator<OrderWide> orderWideStreamWithoutDim = factJoin(sourceStreams);
        //2.维度表join
        SingleOutputStreamOperator<OrderWide> orderWideStreamWithDim = dimJoin(orderWideStreamWithoutDim);
        // 3. 把宽表写入到dwm层(kafka)
        sendToKafka(orderWideStreamWithDim);
    }

    private void sendToKafka(SingleOutputStreamOperator<OrderWide> stream) {
        stream
                .map(JSON::toJSONString)
                .addSink(MykafkaUtil.getKafkaSink(Constant.TOPIC_DWM_ORDER_WIDE));
    }

    private SingleOutputStreamOperator<OrderWide> dimJoin(SingleOutputStreamOperator<OrderWide> orderWideStreamWithoutDim) {
        //异步处理，需要异步的API，最关键的是写一个异步函数
        return AsyncDataStream.unorderedWait(
                orderWideStreamWithoutDim,
                new DimAsyncFunction<OrderWide>() {
                    //主要的操作
                    @Override
                    public void addDim(Connection phoenixConn,
                                       Jedis redisClient,
                                       OrderWide wide,
                                       ResultFuture<OrderWide> resultFuture) throws Exception{

                        // 1. 补充 dim_user_info  select * from t where id=?
                        JSONObject userInfo = DimUtil.readDim(phoenixConn, redisClient, Constant.DIM_USER_INFO, wide.getUser_id().toString());
                        wide.setUser_gender(userInfo.getString("GENDER"));
                        wide.setUser_age(userInfo.getString("BIRTHDAY"));

                        // 2. 省份
                        JSONObject baseProvince = DimUtil.readDim(phoenixConn, redisClient, Constant.DIM_BASE_PROVINCE, wide.getProvince_id().toString());
                        wide.setProvince_3166_2_code(baseProvince.getString("ISO_3166_2"));
                        wide.setProvince_area_code(baseProvince.getString("AREA_CODE"));
                        wide.setProvince_iso_code(baseProvince.getString("ISO_CODE"));
                        wide.setProvince_name(baseProvince.getString("NAME"));

                        // 3. sku
                        JSONObject skuInfo = DimUtil.readDim(phoenixConn, redisClient, Constant.DIM_SKU_INFO, wide.getSku_id().toString());
                        wide.setSku_name(skuInfo.getString("SKU_NAME"));

                        wide.setSpu_id(skuInfo.getLong("SPU_ID"));
                        wide.setTm_id(skuInfo.getLong("TM_ID"));
                        wide.setCategory3_id(skuInfo.getLong("CATEGORY3_ID"));

                        // 4. spu
                        JSONObject spuInfo = DimUtil.readDim(phoenixConn, redisClient, Constant.DIM_SPU_INFO, wide.getSpu_id().toString());
                        wide.setSpu_name(spuInfo.getString("SPU_NAME"));
                        // 5. tm
                        JSONObject tmInfo = DimUtil.readDim(phoenixConn, redisClient, Constant.DIM_BASE_TRADEMARK, wide.getTm_id().toString());
                        wide.setTm_name(tmInfo.getString("TM_NAME"));

                        // 5. c3
                        JSONObject c3Info = DimUtil.readDim(phoenixConn, redisClient, Constant.DIM_BASE_CATEGORY3, wide.getCategory3_id().toString());
                        wide.setCategory3_name(c3Info.getString("NAME"));

                        //往外传结果，但还是不明白   底层？
                        resultFuture.complete(Collections.singletonList(wide));
                    }
                },
                60,
                TimeUnit.SECONDS
        );
    }

    private SingleOutputStreamOperator<OrderWide> factJoin(Map<String, DataStreamSource<String>> sourceStreams) {
        KeyedStream<OrderInfo, Long> InfoStream = sourceStreams.get(Constant.TOPIC_DWD_ORDER_INFO)
                //封装成orderInfo
                .map(value -> JSON.parseObject(value, OrderInfo.class))
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<OrderInfo>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                                .withTimestampAssigner((element, recordTimestamp) -> element.getCreate_ts())
                )
                .keyBy(OrderInfo::getId);
        KeyedStream<OrderDetail, Long> orderDetailStream = sourceStreams.get(Constant.TOPIC_DWD_ORDER_DETAIL)
                .map(value -> JSON.parseObject(value, OrderDetail.class))
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<OrderDetail>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                                .withTimestampAssigner((element, recordTimestamp) -> element.getCreate_ts())
                )
                .keyBy(OrderDetail::getOrder_id);
        //join一般用内连接,前5秒到后5秒
        return InfoStream
                .intervalJoin(orderDetailStream)
                .between(Time.seconds(-5),Time.seconds(5))
                .process(new ProcessJoinFunction<OrderInfo, OrderDetail, OrderWide>() {
                    @Override
                    public void processElement(OrderInfo left, OrderDetail right, Context ctx, Collector<OrderWide> out) throws Exception {
                        out.collect(new OrderWide(left,right));
                    }
                });

    }
}
