package com.missouri.realtime.util;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

/**
 * @author Missouri
 * @date 2021/8/3 19:21
 */
public class RedisUtil {
    //通过连接池来直接获取一个连接对象
    static JedisPool pool;
    //静态代码块写，直接第一次调用就初始化好配置文件，并且只调用一次 并且连接池一定有一个连接对象
    static {
        JedisPoolConfig conf = new JedisPoolConfig();
        conf.setMaxTotal(300);
        conf.setMaxIdle(10);
        conf.setMaxWaitMillis(10000);
        conf.setMinIdle(4);
        conf.setTestOnCreate(true);
        conf.setTestOnBorrow(true);
        conf.setTestOnReturn(true);

        pool = new JedisPool(conf,"hadoop162",6379);
    }

    public static Jedis getRedisClient(){

        Jedis jedis = pool.getResource();
        jedis.select(1); //选择库
        return jedis;
    }
}
