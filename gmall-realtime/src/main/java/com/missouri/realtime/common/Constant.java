package com.missouri.realtime.common;

/**
 * @author Missouri
 * @date 2021/7/28 9:30
 */
public class Constant {

    //kafka的主题
    public static final String TOPIC_ODS_LOG = "ods_log";
    public static final String TOPIC_ODS_DB = "ods_db";
    public static final String TOPIC_DWD_START = "dwd_start";
    public static final String TOPIC_DWD_PAGE = "dwd_page";
    public static final String TOPIC_DWD_DISPLAYS = "dwd_displays";
    //uv和跳出明细
    public static final String TOPIC_DWM_UV = "dwm_uv";
    public static final String TOPIC_DWM_USER_JUMP_DETAIL = "dwm_user_jump_detail";
    public static final String TOPIC_DWD_ORDER_INFO = "dwd_order_info";
    public static final String TOPIC_DWD_ORDER_DETAIL = "dwd_order_detail";

   //表
    //订单宽表
    public static final String DIM_USER_INFO = "DIM_USER_INFO";
    public static final String DIM_BASE_PROVINCE = "DIM_BASE_PROVINCE";
    public static final String DIM_SKU_INFO = "DIM_SKU_INFO";
    public static final String DIM_SPU_INFO = "DIM_SPU_INFO";
    public static final String DIM_BASE_TRADEMARK = "DIM_BASE_TRADEMARK";
    public static final String DIM_BASE_CATEGORY3 = "DIM_BASE_CATEGORY3";



    //hbase
    public static final String PHOENIX_URL = "jdbc:phoenix:hadoop162,hadoop163,hadoop164:2181";
    public static final String PHOENIX_DRIVER = "org.apache.phoenix.jdbc.PhoenixDriver";
    //mysql
    public static final String MYSQL_DRIVER = "com.mysql.jdbc.Driver";
    public static final String MYSQL_URL = "jdbc:mysql://hadoop162:3306/gmall2021?user=root&&password=aaaaaa";
}
