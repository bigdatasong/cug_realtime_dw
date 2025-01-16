package cn.cug.edu.dwd.canceld.app;

import cn.cug.edu.common.base.BaseStreamTableApp;
import cn.cug.edu.common.constant.GmallConstant;
import cn.cug.edu.common.util.SqlUtil;
import com.squareup.okhttp.Cache;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.Duration;

/**
 * author song
 * date 2025-01-12 22:56
 * Desc
 */
public class CancelDetail  extends BaseStreamTableApp {

    public static void main(String[] args) {
        new CancelDetail().start(
                11005,4, GmallConstant.TOPIC_DWD_TRADE_CANCEL_DETAIL
        );
    }

    @Override
    protected void handle(StreamTableEnvironment tableEnv, StreamExecutionEnvironment env) {

        //1.订单从下单到取消，在业务过程中允许的最大的时间间隔。例如在业务中要求，订单从下单后，必须在30min之内支付，否则会被系统自动取消。
        //订单从下单后，在没有被系统自动取消时，可以手动取消。
        tableEnv.getConfig().setIdleStateRetention(Duration.ofMinutes(35));

        //1.读ods层的原始数据(ods_db),名字叫ods_db
        createOdsDB(tableEnv,GmallConstant.TOPIC_DWD_TRADE_CANCEL_DETAIL);
        //2. 获取取消的订单order_info
        String orderInfoSql = " select " +
                "  data['id'] id, " +
                "  data['operate_time'] cancel_time ," +
                "  ts " +
                " from ods_db " +
                " where `database` = 'cug_rt_gmall' " +
                " and `table` = 'order_info' " +
                " and `type` = 'update' " +
                " and `old`['order_status'] = '1001' " +
                " and `data`['order_status'] = '1003'  ";

        tableEnv.createTemporaryView("oi",tableEnv.sqlQuery(orderInfoSql));


        //3.获取dwd_trade_order_detail
        String dwdOrderDetailSql = " create table t1(" +
                " id STRING, " +
                " order_id STRING, " +
                " sku_id STRING ," +
                " sku_num STRING ," +
                " split_total_amount STRING ," +
                " split_activity_amount STRING ," +
                " split_coupon_amount STRING ," +
                " create_time STRING ," +
                "  user_id STRING, " +
                "  province_id STRING," +
                "  coupon_id STRING," +
                "   coupon_use_id STRING ," +
                "  activity_id STRING , " +
                "  `offset` BIGINT METADATA VIRTUAL  ," +
                "  activity_rule_id STRING  " + SqlUtil.getKafkaSourceConfigSql(GmallConstant.TOPIC_DWD_TRADE_ORDER_DETAIL,"230724");

        tableEnv.executeSql(dwdOrderDetailSql);

        //关联
        String joinSql = " select " +
                " t1.id , " +
                " order_id , " +
                " sku_id  ," +
                " sku_num  ," +
                " split_total_amount  ," +
                " split_activity_amount  ," +
                " split_coupon_amount  ," +
                " create_time  ," +
                "  ts  ," +
                "  user_id , " +
                "  province_id ," +
                "  coupon_id ," +
                "  coupon_use_id  ," +
                "  activity_id  ," +
                "  activity_rule_id  , " +
                "  cancel_time " +
                "  from  oi " +
                "  join  t1" +
                "  on oi.id = t1.order_id ";

        /*
            创建sink表
                从TOPIC_DWD_TRADE_ORDER_DETAIL 关联查询，TOPIC_DWD_TRADE_ORDER_DETAIL有撤回，
                会造成我最终写出的结果也有撤回，必须用upsert-kafka写出
         */
        String sinkSql = " create table " + GmallConstant.TOPIC_DWD_TRADE_CANCEL_DETAIL +"(" +
                " id STRING, " +
                " order_id STRING, " +
                " sku_id STRING ," +
                " sku_num STRING ," +
                " split_total_amount STRING ," +
                " split_activity_amount STRING ," +
                " split_coupon_amount STRING ," +
                " create_time STRING ," +
                " ts BIGINT ," +
                "  user_id STRING, " +
                "  province_id STRING," +
                "  coupon_id STRING," +
                "   coupon_use_id STRING ," +
                "  activity_id STRING ," +
                "  activity_rule_id STRING , " +
                "  cancel_time STRING ," +
                "   PRIMARY KEY (id) NOT ENFORCED " + SqlUtil.getUpsertKafkaSinkSql(GmallConstant.TOPIC_DWD_TRADE_CANCEL_DETAIL);

        tableEnv.executeSql(sinkSql);
        tableEnv.executeSql("insert into "+ GmallConstant.TOPIC_DWD_TRADE_CANCEL_DETAIL + joinSql);




    }
}
