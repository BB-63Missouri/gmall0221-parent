package com.missouri.realtime.util;

import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * @author Missouri
 * @date 2021/8/4 17:45
 */
public class ThreadUtil {

    public static ThreadPoolExecutor getThreadPool(){
        return new ThreadPoolExecutor(
                100,//核心线程数
                300,//核心数上限
                1,//保持时间
                TimeUnit.MINUTES,
                new LinkedBlockingDeque<>(100) //超过上限之后的线程存储到这个队列中，可以其他队列
        );
    }
}
