package com.missouri.realtime.util;

import com.missouri.realtime.common.Constant;
import net.minidev.json.JSONObject;
import org.apache.commons.beanutils.BeanUtils;

import java.lang.reflect.InvocationTargetException;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;

/**
 * @author Missouri
 * @date 2021/7/30 19:40
 */
//在queryList sql补齐中，注意phoenix是从1开始索引，fori 默认从0
public class JdbcUtil {
    //下面两个方法为测试
    public static void main(String[] args) throws SQLException, ClassNotFoundException, IllegalAccessException, InvocationTargetException, InstantiationException {
        List<JSONObject> list = JdbcUtil
                .queryList(getMysqlConnection(), "select * from spu_info where id = ?", new Object[]{1}, JSONObject.class);

        for (JSONObject obj : list) {

            System.out.println(obj);
        }
    }
    public static Connection getMysqlConnection() throws ClassNotFoundException, SQLException {
        Class.forName(Constant.MYSQL_DRIVER);
        return DriverManager.getConnection(Constant.MYSQL_URL);
    }

    //在前面加<T>后面也可以设定类型
    public static <T> List<T> queryList(Connection conn, String sql, Object[] args, Class<T> tClass) throws SQLException, IllegalAccessException, InstantiationException, InvocationTargetException {
        List<T> result = new ArrayList<>();
        //连接器查询
        PreparedStatement ps = conn.prepareStatement(sql);
        //1.给传进来的sql占位符赋值
        for (int i = 0; i < args.length; i++) {
            //按照ps的占位符和对应的参数位置赋值
            ps.setObject(i + 1, args[i]);
        }
        //执行sql，得到最终结果 id  name  age
        ResultSet resultSet = ps.executeQuery();
        //resultSet.getMetaData()这个方法是检查对象列的属性，即获取元数据
        ResultSetMetaData metaData = resultSet.getMetaData();
        while(resultSet.next()){
            //newInstance()和New一个作用，创建实例化对象，但此方法需要类已加载，弱类型
            T t = tClass.newInstance();

            for (int i = 1; i <= metaData.getColumnCount(); i++) {
                String columnName= metaData.getColumnLabel(i); //应用在sql字句一般是as后面的，即获取别名
                Object value = resultSet.getObject(columnName);
                BeanUtils.setProperty(t, columnName , value);//借用工具类转换类型，类型t传过来，columnName名字
            }
            result.add(t);
        }
        return result;
    }

    //phoenix连接器获取
    public static Connection getPhoenixConnection() throws SQLException, ClassNotFoundException {
        //通过反射获取jdbc connector？还是不太懂
            Class.forName(Constant.PHOENIX_DRIVER);
        //url获取连接
        return DriverManager.getConnection(Constant.PHOENIX_URL);

    }
}
