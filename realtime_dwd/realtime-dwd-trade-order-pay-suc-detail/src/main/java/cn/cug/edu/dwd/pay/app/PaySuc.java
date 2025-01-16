package cn.cug.edu.dwd.pay.app;

import cn.cug.edu.common.base.BaseStreamTableApp;
import cn.cug.edu.common.constant.GmallConstant;
import cn.cug.edu.common.util.SqlUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * author song
 * date 2025-01-13 5:29
 * Desc
 */
public class PaySuc extends BaseStreamTableApp {

    public static void main(String[] args) {
        new PaySuc().start(
                11006,4, GmallConstant.TOPIC_DWD_TRADE_PAY_DETAIL_SUC
        );
    }

    @Override
    protected void handle(StreamTableEnvironment tableEnv, StreamExecutionEnvironment env) {
        //interval join无需设置ttl，系统会自动维护
        //1.  30min(下单后，30分钟不支付，系统自动取消) + 5min(发起支付到支付被取消的极限时间) +  极限压测条件下的传输消耗时间
        //env.getConfig().setIdleStateRetention(Duration.ofMinutes(35+1) );

        //1.读ods层的原始数据(ods_db),名字叫ods_db
        createOdsDB(tableEnv,GmallConstant.TOPIC_DWD_TRADE_PAY_DETAIL_SUC);

        //2.查询支付成功的信息
        String paymentInfoSql = " select " +
                "  data['id'] id, " +
                "  data['user_id'] user_id, " +
                "  data['order_id'] order_id, " +
                "  data['trade_no'] trade_no, " +
                "  data['total_amount'] total_amount, " +
                "  data['payment_status'] payment_status, " +
                "  data['payment_type'] payment_type, " +
                "  data['callback_time'] callback_time," +
                "  ts ," +
                "  pt, " +
                "  et " +
                " from ods_db " +
                " where `database` = 'cug_rt_gmall' " +
                " and `table` = 'payment_info' " +
                " and `type` = 'update'" +
                " and `old`['payment_status'] = '1601' " +
                " and `data`['payment_status'] = '1602' ";

        tableEnv.createTemporaryView("pi",tableEnv.sqlQuery(paymentInfoSql));

        //3.获取dwd_trade_order_detail
        String dwdOrderDetailSql = " create table od(" +
                " id STRING, " +
                " order_id STRING, " +
                " sku_id STRING ," +
                " sku_num STRING ," +
                " split_total_amount STRING ," +
                " split_activity_amount STRING ," +
                " split_coupon_amount STRING ," +
                " create_time STRING ," +
                "  province_id STRING," +
                "  coupon_id STRING," +
                "   coupon_use_id STRING ," +
                "  activity_id STRING , " +
                "  ts BIGINT ," +
                "  et as TO_TIMESTAMP_LTZ(ts,0) ," +
                "  `offset` BIGINT METADATA VIRTUAL  ," +
                "  activity_rule_id STRING  " + SqlUtil.getKafkaSourceConfigSql(GmallConstant.TOPIC_DWD_TRADE_ORDER_DETAIL,"230724");

        tableEnv.executeSql(dwdOrderDetailSql);

        /*
            举例：  dwd_order_detail topic有以下数据
                                    k            v
                   offset=5, id": "14245187",   "province_id": null,
                   offset=6, id": "14245187",
                   offset=7, id": "14245187",   "province_id": 1,

            查询  ,后续 (select * from od where rn = 1) tmp
         */
       /* String dwdQuerySql = " select" +
            "  * ," +
            "  row_number() over(partition by id order by `offset`  ) rn" +
            " from " + TOPIC_DWD_TRADE_ORDER_DETAIL ;

        env.createTemporaryView("od",env.sqlQuery(dwdQuerySql));*/



        //4.获取维度表  dim_dic_code
        createDicCode(tableEnv);

        /*
            5.关联
                interval join： 必须有事件时间，和水印
                look-up join： 必须有处理时间
         */
        String joinSql = " select" +
                " pi.id, " +
                " user_id, " +
                " pi.order_id, " +
                " trade_no, " +
                " total_amount, " +
                " payment_status, " +
                " payment_type, " +
                " callback_time," +
                " sku_id  ," +
                " sku_num  ," +
                " split_total_amount  ," +
                " split_activity_amount  ," +
                " split_coupon_amount  ," +
                " create_time  ," +
                "  province_id ," +
                "  coupon_id ," +
                "  coupon_use_id  ," +
                "  activity_id  ," +
                "  activity_rule_id  , " +
                "  dim1.info.dic_name payment_status_name ,  " +
                "  dim2.info.dic_name payment_type_name ," +
                "  pi.ts " +
                " from pi " +
                " join od " +
                " on pi.order_id = od.order_id " +
                " and od.et between pi.et - INTERVAL '30' MINUTE and pi.et  " +
                "  left JOIN dim_dic_code FOR SYSTEM_TIME AS OF pi.pt as dim1 " +
                "    ON pi.payment_status = dim1.id " +
                "  left JOIN dim_dic_code FOR SYSTEM_TIME AS OF pi.pt as dim2 " +
                "    ON pi.payment_type = dim2.id ";

        //6.创建sink表
        String sinkSql = "create table "+GmallConstant.TOPIC_DWD_TRADE_PAY_DETAIL_SUC+"("+
                "  id STRING, " +
                "  user_id STRING, " +
                "  order_id STRING, " +
                "  trade_no STRING, " +
                "  total_amount STRING, " +
                "  payment_status STRING, " +
                "  payment_type STRING, " +
                "  callback_time STRING," +
                "  sku_id  STRING ," +
                "  sku_num  STRING ," +
                "  split_total_amount  STRING ," +
                "  split_activity_amount  STRING ," +
                "  split_coupon_amount  STRING ," +
                "  create_time  STRING ," +
                "  province_id STRING ," +
                "  coupon_id STRING ," +
                "  coupon_use_id  STRING ," +
                "  activity_id  STRING ," +
                "  activity_rule_id   STRING, " +
                "  payment_status_name STRING , " +
                "  payment_type_name STRING ," +
                "  ts BIGINT," +
                "  PRIMARY KEY (id) NOT ENFORCED " + SqlUtil.getUpsertKafkaSinkSql(GmallConstant.TOPIC_DWD_TRADE_PAY_DETAIL_SUC);

        tableEnv.executeSql(sinkSql);

        tableEnv.executeSql("insert into "+ GmallConstant.TOPIC_DWD_TRADE_PAY_DETAIL_SUC + joinSql);


    }
}
