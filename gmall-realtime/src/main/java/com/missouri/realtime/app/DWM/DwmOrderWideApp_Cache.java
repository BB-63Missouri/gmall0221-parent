package com.missouri.realtime.app.DWM;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.missouri.realtime.app.BasesApp;
import com.missouri.realtime.bean.OrderDetail;
import com.missouri.realtime.bean.OrderInfo;
import com.missouri.realtime.bean.OrderWide;
import com.missouri.realtime.common.Constant;
import com.missouri.realtime.util.DimUtil;
import com.missouri.realtime.util.JdbcUtil;
import com.missouri.realtime.util.RedisUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import redis.clients.jedis.Jedis;

import java.sql.Connection;
import java.time.Duration;
import java.util.Map;

/**
 * @author Missouri
 * @date 2021/8/3 18:15
 */
/*
    优化1，旁路缓存，由于，每次都要访问hbase，如果和hbase节点距离较远则太慢，又浪费，则可以使用缓存暂存
    缓存策略：
    1.缓存要设过期时间，不然冷数据会常驻缓存浪费资源。
2.要考虑维度数据是否会发生变化，如果发生变化要主动更新缓存。

    有flink状态和redis可选,但又个问题，hbase维度数据发生变化，则不会主动去更新状态的数据,即状态的数据没有更新
    2. 使用专用缓存(redis)

    解决了数据库查询

    网络没解决

    有动态分流程序负责更新redis中的维度

    查询逻辑:
    先查询redis,

    如果redis有, 就从redis直接获取

    如果redis没有, 从Phoenix读取,

    至于更新redis，则由动态分流将数据传输到phoenix的下一步更新,如果维度不更新，动态分流时也不会将数据写到phoenix
 */
public class DwmOrderWideApp_Cache extends BasesApp {

    public static void main(String[] args) {
        new DwmOrderWideApp_Cache().init(
                1,
                "DwmOrderWideApp_Cache",
                "DwmOrderWideApp_Cache",
                20000,
                Constant.TOPIC_DWD_ORDER_INFO,
                Constant.TOPIC_DWD_ORDER_DETAIL
        );
    }

    @Override
    protected void run(StreamExecutionEnvironment env, Map<String, DataStreamSource<String>> sourceStreams) {
        SingleOutputStreamOperator<OrderWide> orderWideWithoutDim = factJoin(sourceStreams);
        dimJoin(orderWideWithoutDim);
    }

    private void dimJoin(SingleOutputStreamOperator<OrderWide> orderWideWithoutDim) {
        orderWideWithoutDim
                //用map处理
                .map(new RichMapFunction<OrderWide, OrderWide>() {

                    private Jedis redisClient;
                    private Connection phoenixConn;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        redisClient = RedisUtil.getRedisClient();
                        phoenixConn = JdbcUtil.getPhoenixConnection();
                    }
                    //关资源，
                    @Override
                    public void close() throws Exception {
                        if (redisClient != null){
                            redisClient.close();
                        }
                        if (phoenixConn != null){
                            phoenixConn.close();
                        }
                    }

                    @Override
                    public OrderWide map(OrderWide wide) throws Exception {
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

                        return wide;
                    }
                });

    }

    private SingleOutputStreamOperator<OrderWide> factJoin(Map<String, DataStreamSource<String>> sourceStreams) {


        KeyedStream<OrderInfo, Long> orderInfoStream = sourceStreams
                .get(Constant.TOPIC_DWD_ORDER_INFO)
                .map(value -> JSONObject.parseObject(value, OrderInfo.class))
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<OrderInfo>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                                //调用工具类获取
                                .withTimestampAssigner((element, recordTimestamp) -> element.getCreate_ts())
                )
                .keyBy(OrderInfo::getId);


        KeyedStream<OrderDetail, Long> orderDetailStream = sourceStreams
                .get(Constant.TOPIC_DWD_ORDER_DETAIL)
                .map(value -> JSONObject.parseObject(value, OrderDetail.class))
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<OrderDetail>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                                .withTimestampAssigner((element, recordTimestamp) -> element.getCreate_ts())
                )
                .keyBy(OrderDetail::getOrder_id);

        return orderInfoStream
                .intervalJoin(orderDetailStream)
                .between(Time.seconds(-5), Time.seconds(5))
                .process(new ProcessJoinFunction<OrderInfo, OrderDetail, OrderWide>() {
                    @Override
                    public void processElement(OrderInfo left,
                                               OrderDetail right,
                                               Context ctx,
                                               Collector<OrderWide> out) throws Exception {
                        out.collect(new OrderWide(left, right));
                    }
                });
    }
}
