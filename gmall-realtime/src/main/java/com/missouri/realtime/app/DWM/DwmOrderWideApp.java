package com.missouri.realtime.app.DWM;

import com.missouri.realtime.app.BasesApp;
import com.missouri.realtime.common.Constant;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Map;

/**
 * @author Missouri
 * @date 2021/8/2 20:27
 */
public class DwmOrderWideApp extends BasesApp {
    //初始化
    public static void main(String[] args) {
        new DwmOrderWideApp().init(
                1,
                "DwmOrderWideApp",
                "DwmOrderWideApp",
                34000,
                Constant.TOPIC_DWD_ORDER_INFO,
                Constant.TOPIC_DWD_ORDER_DETAIL

        );
    }


    @Override
    protected void run(StreamExecutionEnvironment env,
                       Map<String, DataStreamSource<String>> sourceStreams) {
        sourceStreams.get(Constant.TOPIC_DWD_ORDER_INFO).print("info");
        sourceStreams.get(Constant.TOPIC_DWD_ORDER_DETAIL).print("detail");
    }
}
