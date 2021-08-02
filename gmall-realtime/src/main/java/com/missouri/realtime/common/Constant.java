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
    public static final String TOPIC_DWM_UV = "dwm_uv";
    public static final String TOPIC_DWM_USER_JUMP_DETAIL = "dwm_user_jump_detail";
    public static final String TOPIC_DWD_ORDER_INFO = "dwd_order_info";
    public static final String TOPIC_DWD_ORDER_DETAIL = "dwd_order_detail";
    //hbase
    public static final String PHOENIX_URL = "jdbc:phoenix:hadoop162,hadoop163,hadoop164:2181";
    public static final String PHOENIX_DRIVER = "org.apache.phoenix.jdbc.PhoenixDriver";
}
