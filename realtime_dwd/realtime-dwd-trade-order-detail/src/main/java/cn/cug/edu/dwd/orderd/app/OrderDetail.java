package cn.cug.edu.dwd.orderd.app;

import cn.cug.edu.common.base.BaseStreamTableApp;
import cn.cug.edu.common.constant.GmallConstant;
import cn.cug.edu.common.util.SqlUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.Duration;

/**
 * author song
 * date 2025-01-12 22:45
 * Desc
 */
public class OrderDetail  extends BaseStreamTableApp {

    public static void main(String[] args) {
        new OrderDetail().start(11004,4, GmallConstant.TOPIC_DWD_TRADE_ORDER_DETAIL);
    }
    @Override
    protected void handle(StreamTableEnvironment tableEnv, StreamExecutionEnvironment env) {

        //所关联的数据，都是下单业务产生的数据。理论上，应该是同时产生，被采集，被处理
        //进行压力测试，测试同时产生的业务数据，在集群极端繁忙的情况下，最多延迟多久，可以到达flink程序。
        tableEnv.getConfig().setIdleStateRetention(Duration.ofSeconds(10));

        //1.读ods层的原始数据(ods_db),名字叫ods_db
        createOdsDB(tableEnv,GmallConstant.TOPIC_DWD_TRADE_ORDER_DETAIL);
        //2.创建4张和下单相关的业务表
        String orderInfoSql = " select " +
                "  data['id'] id, " +
                "  data['user_id'] user_id, " +
                "  data['province_id'] province_id " +
                " from ods_db " +
                " where `database` = 'cug_rt_gmall' " +
                " and `table` = 'order_info' " +
                " and `type` = 'insert' ";

        String orderDetailSql = " select " +
                "  data['id'] id, " +
                "  data['order_id'] order_id, " +
                "  data['sku_id'] sku_id ," +
                "  data['sku_num'] sku_num ," +
                "  data['split_total_amount'] split_total_amount ," +
                "  data['split_activity_amount'] split_activity_amount ," +
                "  data['split_coupon_amount'] split_coupon_amount ," +
                "  data['create_time'] create_time ," +
                "  ts " +
                " from ods_db " +
                " where `database` = 'cug_rt_gmall' " +
                " and `table` = 'order_detail' " +
                " and `type` = 'insert' ";

        String couponDetailSql = " select " +
                "  data['order_detail_id'] order_detail_id, " +
                "  data['coupon_id'] coupon_id ," +
                "  data['coupon_use_id'] coupon_use_id " +
                " from ods_db " +
                " where `database` = 'cug_rt_gmall' " +
                " and `table` = 'order_detail_coupon' " +
                " and `type` = 'insert' ";

        String activityDetailSql = " select " +
                "  data['order_detail_id'] order_detail_id, " +
                "  data['activity_id'] activity_id ," +
                "  data['activity_rule_id'] activity_rule_id " +
                " from ods_db " +
                " where `database` = 'cug_rt_gmall' " +
                " and `table` = 'order_detail_activity' " +
                " and `type` = 'insert' ";

        tableEnv.createTemporaryView("od",tableEnv.sqlQuery(orderDetailSql));
        tableEnv.createTemporaryView("oi",tableEnv.sqlQuery(orderInfoSql));
        tableEnv.createTemporaryView("cd",tableEnv.sqlQuery(couponDetailSql));
        tableEnv.createTemporaryView("ad",tableEnv.sqlQuery(activityDetailSql));

        //3.关联
        String joinSql = " select " +
                "  od.* , " +
                "  user_id, " +
                "  province_id," +
                "  coupon_id," +
                "   coupon_use_id ," +
                "  activity_id ," +
                "  activity_rule_id " +
                " from od " +
                " left join oi on od.order_id = oi.id " +
                " left join cd on od.id = cd.order_detail_id " +
                " left join ad on od.id = ad.order_detail_id ";

        /*
            4.创建sink表
                    left join: 结果可能会有 -D，不能使用普通的kafka连接器写出，只能使用upsert-kafka连接器。
                        upsert-kafka必须要求有主键约束。
                            主键作为 Record中的key。

                            +I:   Record(K,V)
                            -D:   Record(K,null)
         */
        String sinkSql = " create table " + GmallConstant.TOPIC_DWD_TRADE_ORDER_DETAIL +"(" +
                " id STRING, " +
                " order_id STRING, " +
                " sku_id STRING ," +
                " sku_num STRING ," +
                " split_total_amount STRING ," +
                " split_activity_amount STRING ," +
                " split_coupon_amount STRING ," +
                " create_time STRING ," +
                "  ts BIGINT ," +
                "  user_id STRING, " +
                "  province_id STRING," +
                "  coupon_id STRING," +
                "   coupon_use_id STRING ," +
                "  activity_id STRING ," +
                "  activity_rule_id STRING , " +
                "   PRIMARY KEY (id) NOT ENFORCED " + SqlUtil.getUpsertKafkaSinkSql(GmallConstant.TOPIC_DWD_TRADE_ORDER_DETAIL);

        tableEnv.executeSql(sinkSql);

        //写出
        tableEnv.executeSql("insert into "+ GmallConstant.TOPIC_DWD_TRADE_ORDER_DETAIL + joinSql);

    }

    }

