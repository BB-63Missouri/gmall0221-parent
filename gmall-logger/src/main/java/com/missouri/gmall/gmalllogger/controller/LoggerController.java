package com.missouri.gmall.gmalllogger.controller;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/**
 * @author Missouri
 * @date 2021/7/26 17:36
 */
@Slf4j
@RestController //返回值为响应体不是地址
public class LoggerController {
//
    @GetMapping("/applog")//提交路径
    //@RequestParam请求参数,传过来的值赋值给logString
    public String doLog(@RequestParam("param") String logString){
        //日志落盘（离线需求）用lf4j落盘。
        saveToDisk(logString);
        //数据写入kafka
        sendToKafka(logString);

        return "ok";
    }
    //传输去kafka
    //KafkaTemplate 为 kafka的模板，Autowired自动注入
    @Autowired
    private KafkaTemplate<String,String> kafka;
    //写到ods_log主题
    private void sendToKafka(String logString){
        kafka.send("ods_log",logString);
    }


    private void saveToDisk(String logString){
        log.info(logString);
    }
}
