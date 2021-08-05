package com.missouri.realtime.function;

import com.missouri.realtime.util.JdbcUtil;
import com.missouri.realtime.util.RedisUtil;
import com.missouri.realtime.util.ThreadUtil;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import redis.clients.jedis.Jedis;


import java.lang.reflect.InvocationTargetException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.concurrent.ThreadPoolExecutor;

/**
 * @author Missouri
 * @date 2021/8/4 17:04
 */
public abstract class DimAsyncFunction<T> extends RichAsyncFunction<T,T> {

    //使用线程池代替客户端
    private ThreadPoolExecutor threadPool;
    private Connection phoenixConn;

    @Override
    public void open(Configuration parameters) throws Exception {
        threadPool = ThreadUtil.getThreadPool();
        phoenixConn = JdbcUtil.getPhoenixConnection();
    }

    @Override
    public void close() throws Exception {
        if (phoenixConn != null){
            phoenixConn.close();
        }
        if (threadPool != null){
            threadPool.shutdown();
        }
    }
    //异步函数只要实现这个方法便可
    @Override
    public void asyncInvoke(T input,
                            ResultFuture<T> resultFuture) throws Exception {
    //如果支持客户端API，则选用，无可用线程池代替
        threadPool.submit(()->{
            // 读取维度操作放在这里就可以了

            // redis在异步使用的时候, 必须每个操作单独得到一个客户端
            Jedis redisClient = RedisUtil.getRedisClient();

            //具体业务
            try {
                addDim(phoenixConn,redisClient,input,resultFuture);
            } catch (Exception e) {
                e.printStackTrace();
                throw new RuntimeException("异步执行的时候的异常, " +
                        "请检测异步操作: hbase是否开启, redis是否开启, hadoop是否开启, maxwell是否开启 ....");
            }

            //一个操作结束关闭客户端节省资源
            redisClient.close();
        });
    }

    public abstract void addDim(Connection phoenixConn,
                                Jedis redisClient,
                                T input,
                                ResultFuture<T> resultFuture) throws InvocationTargetException, SQLException, InstantiationException, IllegalAccessException, Exception;

    @Override
    public void timeout(T input, ResultFuture<T> resultFuture) throws Exception {
        System.out.println("超时：" + input);
    }
}
