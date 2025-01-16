package cn.cug.edu.dwd.orderfund.app;

import cn.cug.edu.common.base.BaseStreamTableApp;
import cn.cug.edu.common.constant.GmallConstant;
import cn.cug.edu.common.util.SqlUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.Duration;

/**
 * author song
 * date 2025-01-13 5:36
 * Desc
 */
public class OrderRefund extends BaseStreamTableApp {

    public static void main(String[] args) {
        new OrderRefund().start(
                11008,4, GmallConstant.TOPIC_DWD_TRADE_ORDER_REFUND
        );
    }
    @Override
    protected void handle(StreamTableEnvironment tableEnv, StreamExecutionEnvironment env) {

        //当申请退单，商家同意后。order_refund_info和order_info的update是同步进行。
        tableEnv.getConfig().setIdleStateRetention(Duration.ofSeconds(10));

        //1.读ods层的原始数据(ods_db),名字叫ods_db
        createOdsDB(tableEnv,GmallConstant.TOPIC_DWD_TRADE_ORDER_REFUND);

        //2.查询申请退单成功的退单信息
        String orderRefundSql = " select " +
                "  data['id'] id, " +
                "  data['user_id'] user_id, " +
                "  data['order_id'] order_id, " +
                "  data['sku_id'] sku_id, " +
                "  data['refund_type'] refund_type, " +
                "  data['refund_num'] refund_num, " +
                "  data['refund_amount'] refund_amount, " +
                "  data['refund_reason_type'] refund_reason_type," +
                "  data['refund_reason_txt'] refund_reason_txt," +
                "  data['operate_time'] operate_time," +
                "  ts ," +
                "  pt " +
                " from ods_db " +
                " where `database` = 'cug_rt_gmall' " +
                " and `table` = 'order_refund_info' " +
                " and `type` = 'update'" +
                " and `old`['refund_status'] = '0701' " +
                " and `data`['refund_status'] = '0702' ";

        //tableEnv.sqlQuery(orderRefundSql).execute().print();

        tableEnv.createTemporaryView("ro",tableEnv.sqlQuery(orderRefundSql));

        //3.获取退单的订单信息
        String orderInfoSql = " select " +
                "  data['id'] id, " +
                "  data['province_id'] province_id " +
                " from ods_db " +
                " where `database` = 'cug_rt_gmall' " +
                " and `table` = 'order_info' " +
                " and `type` = 'update' " +
                " and `data`['order_status'] = '1005' "+
                " and `old`['order_status'] is not null "; //保证此次update，update是order_status字段

        tableEnv.createTemporaryView("oi",tableEnv.sqlQuery(orderInfoSql));


        //4.获取维度表  dim_dic_code
        createDicCode(tableEnv);

        /*
            5.关联
         */
        String joinSql = " select" +
                " ro.id, " +
                " user_id, " +
                " order_id, " +
                " sku_id, " +
                " refund_type, " +
                " refund_num, " +
                " refund_amount, " +
                " refund_reason_type," +
                " refund_reason_txt," +
                " operate_time," +
                " province_id ," +
                "  dim1.info.dic_name refund_type_name  ,  " +
                "  dim2.info.dic_name refund_reason_type_name ," +
                "  ts " +
                " from ro " +
                " join oi " +
                " on ro.order_id = oi.id " +
                "  left JOIN dim_dic_code FOR SYSTEM_TIME AS OF ro.pt as dim1 " +
                "    ON ro.refund_type = dim1.id " +
                "  left JOIN dim_dic_code FOR SYSTEM_TIME AS OF ro.pt as dim2 " +
                "    ON ro.refund_reason_type = dim2.id ";

        //6.创建sink表
        String sinkSql = "create table "+GmallConstant.TOPIC_DWD_TRADE_ORDER_REFUND+"("+
                " id STRING, " +
                " user_id STRING, " +
                " order_id STRING, " +
                " sku_id STRING, " +
                " refund_type STRING, " +
                " refund_num STRING, " +
                " refund_amount STRING, " +
                " refund_reason_type STRING," +
                " refund_reason_txt STRING," +
                " operate_time STRING," +
                " province_id STRING ," +
                "  refund_type_name STRING  ,  " +
                "  refund_reason_type_name STRING  ," +
                "  ts BIGINT " + SqlUtil.getKafkaSinkSql(GmallConstant.TOPIC_DWD_TRADE_ORDER_REFUND);

        tableEnv.executeSql(sinkSql);

        tableEnv.executeSql("insert into "+ GmallConstant.TOPIC_DWD_TRADE_ORDER_REFUND + joinSql);


    }
}
