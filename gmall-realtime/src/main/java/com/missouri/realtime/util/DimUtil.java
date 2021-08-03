package com.missouri.realtime.util;

import com.alibaba.fastjson.JSONObject;

import java.lang.reflect.InvocationTargetException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;

/**
 * @author Missouri
 * @date 2021/8/3 9:52
 */
//获取hbase的工具类
public class DimUtil {


    //从phoenix读数据，需要一个连接器，表名，id名是目标名字,类型？
    public static JSONObject readDimFromPhoenix(Connection phoenixConn, String tableName,String id) throws InvocationTargetException, SQLException, InstantiationException, IllegalAccessException {
        //通过jdbc获取phoenix的数据
        String sql = "select * from" + tableName +" where id = ? ";
        List<JSONObject> list = JdbcUtil.<JSONObject>queryList(phoenixConn, sql, new Object[]{id}, JSONObject.class);
        //为什么是第一个选择
        return list.size() == 0 ? new JSONObject() : list.get(0);
    }

    //从redis读数据


}
