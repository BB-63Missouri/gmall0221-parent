package com.missouri.realtime.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @author Missouri
 * @date 2021/7/30 9:08
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class TableProcess {
        //动态分流Sink常量
        public static final String SINK_TYPE_HBASE = "hbase";
        public static final String SINK_TYPE_KAFKA = "kafka";
        public static final String SINK_TYPE_CK = "clickhouse";
        //来源表
        private String source_table;
        //操作类型 insert,update,delete
        private String operate_type;
        //输出类型 hbase kafka
        private String sink_type;
        //输出表(主题)
        private String sink_table;
        //输出字段
        private String sink_columns;
        //主键字段
        private String sink_pk;
        //建表扩展
        private String sink_extend;
}
