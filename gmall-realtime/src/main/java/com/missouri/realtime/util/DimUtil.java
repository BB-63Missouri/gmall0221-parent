package com.missouri.realtime.util;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import redis.clients.jedis.Jedis;

import java.lang.reflect.InvocationTargetException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;

/**
 * @author Missouri
 * @date 2021/8/3 9:52
 */
//java的sql必须慎之又慎，空格得打醒12分精神
//获取hbase的工具类
public class DimUtil {

    //dim读取
    public static JSONObject readDim(Connection phoenixConn, Jedis client, String tableName, String id) throws SQLException, InstantiationException, IllegalAccessException, InvocationTargetException {

        //1.先从redis读取数据
        JSONObject data = readDimFromRedis(phoenixConn, client, tableName, id);

        if (data != null){
            System.out.println("走缓存: " + tableName + "  " + id);
            return data;
        }else {
            System.out.println("走数据库: " + tableName + "  " + id);
            //从phoenix更新数据,再写到redis中
            data = readDimFromPhoenix(phoenixConn, tableName, id);
            System.out.println("phoenix: " + data);
            //写到redis中
            writeDimFromRedis(client,tableName,id,data);
            return data;
        }
    }

    //从phoenix读数据，需要一个连接器，表名，id名是目标名字,类型？
    public static JSONObject readDimFromPhoenix(Connection phoenixConn, String tableName,String id) throws InvocationTargetException, SQLException, InstantiationException, IllegalAccessException {
        //通过jdbc获取phoenix的数据
        String sql = "select * from " + tableName +" where id = ? ";
        List<JSONObject> list = JdbcUtil.<JSONObject>queryList(phoenixConn, sql, new Object[]{id}, JSONObject.class);
        //为什么是第一个选择
        return list.size() == 0 ? new JSONObject() : list.get(0);
    }

    //从redis读数据
    public static JSONObject readDimFromRedis(Connection phoenixConn, Jedis client, String tableName,String id){
        String key = getRedisDimKey(tableName, id);
        String value = client.get(key);
        if (value != null){
            // 每个key每次读到，把过期重新设置24小时
            client.expire(key, 24 * 6 * 60);
            return JSON.parseObject(value);
        }
        return null;
    }

    //从redis更新维度数据
    public static void writeDimFromRedis(Jedis client, String tableName, String id, JSONObject data){
        String key = getRedisDimKey(tableName, id);
        String value = data.toString();
        client.setex(key,24 * 60 * 60 , value );
    }

    private static String getRedisDimKey(String tableName, String id) {
        //key的数据格式
        return tableName + ":" + id;
    }


}
