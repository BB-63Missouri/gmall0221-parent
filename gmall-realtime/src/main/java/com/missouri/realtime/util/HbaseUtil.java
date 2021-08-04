package com.missouri.realtime.util;

import com.alibaba.fastjson.JSONObject;
import com.missouri.realtime.bean.TableProcess;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import redis.clients.jedis.Jedis;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Map;

/**
 * @author Missouri
 * @date 2021/7/30 19:37
 *  通过Phoenix写入hbase
 *  * <p>
 *  * Phoenix支持sql, 所以连接Phoenix可以使用标准的jdbc
 *  * <p>
 *  * 1. flink提供了一个jdbcSink, 可以对JdbcSink做封装, 来实现PhoenixSink
 *  * <p>
 *  * ----
 *  * <p>
 *  * 1. 建表
 *  * <p>
 *  * 2. 写数据
 */
//用到hbase和phoenix,hbase:start-hbase.sh和stop-hbase.sh命令开启hbase;
// phoenix:sqlline.py hadoop163,hadoop164,hadoop162:2181 后面接zk地址
//维度表不常变，如果此前消费过数据，需要maxwell执行重新查询
    /*
bin/maxwell-bootstrap --user maxwell  --password aaaaaa --host hadoop162  --database gmall2021 --table user_info --client_id maxwell_1

bin/maxwell-bootstrap --user maxwell  --password aaaaaa --host hadoop162  --database gmall2021 --table base_province --client_id maxwell_1

bin/maxwell-bootstrap --user maxwell  --password aaaaaa --host hadoop162  --database gmall2021 --table sku_info --client_id maxwell_1

bin/maxwell-bootstrap --user maxwell  --password aaaaaa --host hadoop162  --database gmall2021 --table spu_info --client_id maxwell_1

bin/maxwell-bootstrap --user maxwell  --password aaaaaa --host hadoop162  --database gmall2021 --table base_category3 --client_id maxwell_1

bin/maxwell-bootstrap --user maxwell  --password aaaaaa --host hadoop162  --database gmall2021 --table base_trademark --client_id maxwell_1
     */
public class HbaseUtil {
    public static SinkFunction<Tuple2<JSONObject, TableProcess>> getPhoenixSink(){
        return new PhoenixSink();
    }
    public static class PhoenixSink extends RichSinkFunction<Tuple2<JSONObject, TableProcess>>{
        private Connection conn;
        private ValueState<String> tableCreateState;
        private Jedis client;
        //
        @Override
        public void open(Configuration parameters) throws Exception {
            //加载驱动
            conn = JdbcUtil.getPhoenixConnection();
            //初始化状态
            tableCreateState = getRuntimeContext().getState(new ValueStateDescriptor<String>("tableCreateState", String.class));

            client = RedisUtil.getRedisClient();
        }
        //标准的jdbc来完成
        @Override
        public void invoke(Tuple2<JSONObject, TableProcess> value, Context context) throws Exception {
             //通过状态检测hbase里是否有表，状态没有值则无表，没有则通过SQL建表
            checkTable(value);
            //把数据写入phoenix中,sql插入
            writeToPhoenix(value);
            //更新redis(读取维度数据的优化，旁路缓存)
            //粗暴：可以直接删除缓存,让直接去hbase查
            //更新数据，不新增，但是不能加热点数据，热点数据怎么开始在
            updateCache(value);
        }

        private void updateCache(Tuple2<JSONObject, TableProcess> value) {
            JSONObject data = new JSONObject();
            //将里面的key全部变成大写，hbase里的表名全是大写 ,将数据存入data容器
            for (Map.Entry<String,Object> entry : value.f0.entrySet()){
                data.put(entry.getKey().toUpperCase(),entry.getValue());
            }

            //更新redis缓存,update,redis有维度数据
            String operateType = value.f1.getOperate_type();
            String key = value.f1.getSink_table().toUpperCase();
            //判断是否有数据
            String dim = client.get(key);
            if ("update".equals(operateType) && dim!= null){
                client.setex(key,24 * 60 * 60, data.toJSONString());
            }
        }

        private void writeToPhoenix(Tuple2<JSONObject, TableProcess> value) throws SQLException {
            JSONObject jsonObject = value.f0;
            TableProcess tp = value.f1;
            StringBuilder insertSql = new StringBuilder();
            insertSql
                    //插入只有upsert，没有insert
                    .append("upsert into ")
                    .append(tp.getSink_table())
                    .append("(")
                    //字段名,刚好columns的数据是，分隔
                    .append(tp.getSink_columns())
                    //sql语句拼接还是得细心，漏了s就很难排查
                    .append(")values(")
                    //拼占位符
                    .append(tp.getSink_columns().replaceAll("[^,]+","?"))
                    .append(")");
            PreparedStatement ps = conn.prepareStatement(insertSql.toString());
            //获取占位符的数组
            String[] columnNames = tp.getSink_columns().split(",");
            for (int i = 0; i < columnNames.length; i++) {
                Object v = jsonObject.get(columnNames[i]);
                //有部分数据本身就是null,则要判断然后赋值
                ps.setString(i+1, v == null ? "" : v.toString());
            }
            ps.execute();
            conn.commit();
            ps.close();
        }



        private void checkTable(Tuple2<JSONObject, TableProcess> value) throws IOException, SQLException {
            if(tableCreateState.value() == null){
                //获取数据，补齐sql
                TableProcess tp = value.f1;
                StringBuilder createSql = new StringBuilder();
                //拼接sql
                //sql拼接和替换得注意，自己错了
                createSql
                        .append("create table if not exists ")
                        .append(tp.getSink_table())
                        .append("(")
                        //(tp.getSink_columns().replaceAll(","," varchar")直接将,转为需要的 varchar
                        .append(tp.getSink_columns().replaceAll(","," varchar,"))
                        .append(" varchar, constraint pk primary key(")
                        .append(tp.getSink_pk() == null ? "id" : tp.getSink_pk())
                        .append("))")
                        //分区
                        .append(tp.getSink_extend() == null ? "" : tp.getSink_extend());
                System.out.println(createSql.toString());
                //prepareStatement?
                PreparedStatement ps = conn.prepareStatement(createSql.toString());
                ps.execute();
                conn.commit();
                ps.close();
                //最后更新状态，
                tableCreateState.update(tp.getSink_table());
            }
        }
    }

}
