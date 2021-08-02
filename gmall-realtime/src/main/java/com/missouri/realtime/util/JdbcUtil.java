package com.missouri.realtime.util;

import com.missouri.realtime.common.Constant;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

/**
 * @author Missouri
 * @date 2021/7/30 19:40
 */
public class JdbcUtil {
    public static Connection getPhoenixConnection() throws SQLException, ClassNotFoundException {
        //通过反射获取jdbc connector？还是不太懂
            Class.forName(Constant.PHOENIX_DRIVER);

        //url获取连接
        return DriverManager.getConnection(Constant.PHOENIX_URL);

    }
}
