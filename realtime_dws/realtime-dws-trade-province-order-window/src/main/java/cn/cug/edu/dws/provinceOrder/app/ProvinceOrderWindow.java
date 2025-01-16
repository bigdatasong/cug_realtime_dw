package cn.cug.edu.dws.provinceOrder.app;

import cn.cug.edu.common.base.BaseStreamTableApp;
import cn.cug.edu.common.constant.GmallConstant;
import cn.cug.edu.common.util.SqlUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * author song
 * date 2025-01-16 18:10
 * Desc
 * 使用flinksql，不进行排名过滤。
 *          解决方法一:
 *                             K                  V
 *          offset=5, id": "14245187",   "province_id": null, coupon_id = null
 *          offset=6, id": "14245187",
 *          offset=7, id": "14245187",   "province_id": 1,  coupon_id = null
 *          offset=8, id": "14245187",
 *          offset=9, id": "14245187",   "province_id": 1,  coupon_id = 1
 *          0. 先把province_id 为null的过滤掉(省略),过滤后依旧有重复
 *          1. 在窗口中，只对sku_id，order_detail_id 进行聚合
 *                 offset=7, id": "14245187",   "province_id": 1,  coupon_id = null
 *                 offset=9, id": "14245187",   "province_id": 1,  coupon_id = 1
 *
 *          2. 按照K(order_detail_id)开一个keyed窗口(可以使用TVF)，采取 avg(split_total_amount)进行聚合。
 *
 *          3. 还需要二次聚合
 *          在flink中二次聚合
 *              select  window_start,window_end,sku_id,sum(split_total_amount)  from tmp group by window_start,window_end,sku_id
 *              如何保证flink程序挂掉，Doris的结果不重复？
 *              Doris中value列选择 REPLACE
 *              可以不开启2PC提交。幂等输出。
 *
 *          在Doris中二次聚合
 *             表模型必须是 aggregate，value列，要选择SUM类型
 *
 *          如何保证flink程序挂掉，Doris的结果不重复？
 *          开启2PC提交。
 *
 */
public class ProvinceOrderWindow extends BaseStreamTableApp {

    public static void main(String[] args) {
        new ProvinceOrderWindow().start(
                13110,4, GmallConstant.DWS_TRADE_PROVINCE_ORDER_WINDOW
        );
    }
    
    @Override
    protected void handle(StreamTableEnvironment tableEnv, StreamExecutionEnvironment env) {

        //1.从dwd_order_detail 读取数据
        String odSql = " create table t1(" +
                " id STRING, " +
                " province_id STRING ," +
                " order_id STRING ," +
                " split_total_amount STRING ," +
                " create_time TIMESTAMP(0) ," +
                "  WATERMARK FOR create_time as create_time - INTERVAL '0.001' SECOND " +
                SqlUtil.getKafkaSourceConfigSql(GmallConstant.TOPIC_DWD_TRADE_ORDER_DETAIL,GmallConstant.DWS_TRADE_PROVINCE_ORDER_WINDOW);

        tableEnv.executeSql(odSql);


        //tableEnv.createTemporaryView("t2",tableEnv.sqlQuery(" select * from t2 where rn = 1"));

        /*
            首次聚合，目的是去重。结果是每一个order_detail_id只有一行
                业务粒度的问题:
                        order_info:  一笔订单是一行。   单次 = 单数
                        order_detail:  一笔订单是N行。  单次 = 单数 = count(distinct order_id)
                                      一笔订单的一个商品是一行。

         */
        String tumbleSql = " SELECT " +
                "  window_start stt," +
                "  window_end edt," +
                "  id, " +
                "  province_id," +
                "   order_id ," +
                "  avg( cast(split_total_amount as decimal(16,2)) ) split_total_amount " +
                "  FROM TABLE(" +
                "    TUMBLE(TABLE t1, DESCRIPTOR(create_time), INTERVAL '5' second )" +
                "   ) " +
                "  WHERE province_id is not null " +
                "  GROUP BY window_start, window_end , id  ,province_id ,order_id  ";

        tableEnv.createTemporaryView("t2",tableEnv.sqlQuery(tumbleSql));
        //第二次聚合，统计下单次数和下单金额
        String aggSql = " select " +
                " stt, edt,province_id, " +
                "  TO_DATE(DATE_FORMAT(stt,'yyyy-MM-dd')) cur_date," +
                "  sum(split_total_amount) order_amount ," +
                "  count(distinct order_id ) order_count ," +
                "  PROCTIME() pt " +
                " from t2 " +
                " group by stt, edt,province_id ";

        tableEnv.createTemporaryView("t3",tableEnv.sqlQuery(aggSql));


        //关联维度  使用look-upjoin
        String pSql = " create table p( " +
                "  id STRING ," +
                "  info Row<name STRING > ," +
                "  PRIMARY KEY (id) NOT ENFORCED  " +
                SqlUtil.getHbaseSourceSql("cug_rt_gmall:dim_base_province");

        tableEnv.executeSql(pSql);

        String joinSql = " select " +
                " `stt`                 ," +
                "`edt`                  ," +
                " `cur_date`             ," +
                " cast(province_id as SMALLINT) `province_id`  ," +
                " p.info.name `province_name`       ," +
                " order_count , " +
                "`order_amount` " +
                " from t3 " +
                " left join p FOR SYSTEM_TIME AS OF t3.pt " +
                " ON t3.province_id = p.id " ;

        //5.创建sink表
        String sinkSql = " create table t4 (" +
                "  `stt`            TIMESTAMP,       " +
                " `edt`                TIMESTAMP,   " +
                " `cur_date`         DATE,     " +
                " `province_id`     SMALLINT,     " +
                " `province_name`     STRING,   " +
                " `order_count`      BIGINT,    " +
                " `order_amount`     DECIMAL(16,2) "+ SqlUtil.getDorisSinkSql("cug_rt_gmall.dws_trade_province_order_window");

        tableEnv.executeSql(sinkSql);
        tableEnv.executeSql("insert into t4 "+ joinSql);


    }
}
