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

import java.sql.Connection;
import java.time.Duration;
import java.util.Map;

/**
 * @author Missouri
 * @date 2021/8/2 20:27
 */
//为什么还要开maxwell,有数据,但是重新产数据没事情
public class DwmOrderWideApp extends BasesApp {
    //初始化
    public static void main(String[] args) {
        new DwmOrderWideApp().init(
                1,
                "DwmOrderWideApp",
                "DwmOrderWideApp",
                20000,
                Constant.TOPIC_DWD_ORDER_INFO,
                Constant.TOPIC_DWD_ORDER_DETAIL

        );
    }


    @Override
    protected void run(StreamExecutionEnvironment env,
                       Map<String, DataStreamSource<String>> sourceStreams) {
        //事实表join
        SingleOutputStreamOperator<OrderWide> orderWideStreamWithoutDim = factJoin(sourceStreams);
        SingleOutputStreamOperator<OrderWide> orderWideStreamWithDim = dimJoin(orderWideStreamWithoutDim);
        orderWideStreamWithDim.print();
        //剩下的为传送
    }


    //将事实表join后的表和维度表join， 每一个OrderWide都去hbase里查询相应的维度
    private SingleOutputStreamOperator<OrderWide> dimJoin(SingleOutputStreamOperator<OrderWide> factJoinStream) {
        return factJoinStream
                .map(new RichMapFunction<OrderWide, OrderWide>() {
                    private Connection phoenixConn;
                    @Override
                    public void open(Configuration parameters) throws Exception {
                        //初始化phoenix连接器
                        phoenixConn = JdbcUtil.getPhoenixConnection();
                    }

                    @Override
                    public OrderWide map(OrderWide wide) throws Exception {
                        //1、 dim_user_info  select * from t where id=?
                        JSONObject userInfo = DimUtil.readDimFromPhoenix(phoenixConn, Constant.DIM_USER_INFO, wide.getUser_id().toString());
                        //筛选需要的字段改大写存入hbase
                        wide.setUser_gender(userInfo.getString("GENDER"));
                        wide.setUser_age(userInfo.getString("BIRTHDAY"));

                        //2、省份
                        JSONObject baseProvince = DimUtil.readDimFromPhoenix(phoenixConn, Constant.DIM_BASE_PROVINCE, wide.getProvince_id().toString());
                        wide.setProvince_area_code(baseProvince.getString("AREA_CODE"));
                        wide.setProvince_3166_2_code(baseProvince.getString("ISO_3166_2"));
                        wide.setProvince_iso_code(baseProvince.getString("ISO_CODE"));
                        wide.setProvince_name(baseProvince.getString("NAME"));

                        // 3. sku
                        JSONObject skuInfo = DimUtil.readDimFromPhoenix(phoenixConn, Constant.DIM_SKU_INFO, wide.getSku_id().toString());
                        wide.setSku_name(skuInfo.getString("SKU_NAME"));

                        wide.setSpu_id(skuInfo.getLong("SPU_ID"));
                        wide.setTm_id(skuInfo.getLong("TM_ID"));
                        wide.setCategory3_id(skuInfo.getLong("CATEGORY3_ID"));

                        // 4. spu
                        JSONObject spuInfo = DimUtil.readDimFromPhoenix(phoenixConn, Constant.DIM_SPU_INFO, wide.getSpu_id().toString());
                        wide.setSpu_name(spuInfo.getString("SPU_NAME"));
                        // 5. tm
                        JSONObject tmInfo = DimUtil.readDimFromPhoenix(phoenixConn, Constant.DIM_BASE_TRADEMARK, wide.getTm_id().toString());
                        wide.setTm_name(tmInfo.getString("TM_NAME"));

                        // 5. c3
                        JSONObject c3Info = DimUtil.readDimFromPhoenix(phoenixConn, Constant.DIM_BASE_CATEGORY3, wide.getCategory3_id().toString());
                        wide.setCategory3_name(c3Info.getString("NAME"));

                        return wide;
                    }
                });
    }

    private SingleOutputStreamOperator<OrderWide> factJoin(Map<String, DataStreamSource<String>> sourceStreams) {
        //获取需要join的流
        KeyedStream<OrderInfo, Long> orderInfoLongKeyedStream = sourceStreams
                .get(Constant.TOPIC_DWD_ORDER_INFO)
                //既然是用工具类，为何，转为JSONObject
                .map(value -> JSON.parseObject(value, OrderInfo.class))
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<OrderInfo>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                                .withTimestampAssigner((element, recordTimestamp) -> element.getCreate_ts())
                )
                .keyBy(OrderInfo::getId);

        KeyedStream<OrderDetail, Long> orderDetailLongKeyedStream = sourceStreams
                .get(Constant.TOPIC_DWD_ORDER_DETAIL)
                .map(value -> JSONObject.parseObject(value, OrderDetail.class))
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<OrderDetail>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                                .withTimestampAssigner((element, recordTimestamp) -> element.getCreate_ts())
                )
                .keyBy(OrderDetail::getOrder_id);

        return orderInfoLongKeyedStream
                //join分为窗口join和intervalJoin两种，在flink应用内join
                .intervalJoin(orderDetailLongKeyedStream)
                .between(Time.seconds(-5),Time.seconds(5))
                .process(new ProcessJoinFunction<OrderInfo, OrderDetail, OrderWide>() {
                    @Override
                    public void processElement(OrderInfo left, OrderDetail right, Context ctx, Collector<OrderWide> out) throws Exception {
                        out.collect(new OrderWide(left,right));
                    }
                });
    }
}
