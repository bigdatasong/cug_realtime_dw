package cn.cug.edu.dws.skuorder.app;

import cn.cug.edu.common.base.BaseDataStreamApp;
import cn.cug.edu.common.base.BaseStreamTableApp;
import cn.cug.edu.common.constant.GmallConstant;
import cn.cug.edu.common.util.SqlUtil;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * author song
 * date 2025-01-16 17:57
 * Desc
 */
public class DwsTradeSkuOrderWindowSyncCache extends BaseStreamTableApp {

    public static void main(String[] args) {
        new DwsTradeSkuOrderWindowSyncCache().start(
                13009,4, GmallConstant.DWS_TRADE_SKU_ORDER_WINDOW
        );
    }

    @Override
    protected void handle(StreamTableEnvironment tableEnv, StreamExecutionEnvironment env) {

        //1.从dwd_order_detail 读取数据
        String odSql = " create table t1(" +
                " id STRING, " +
                " sku_id STRING ," +
                " sku_num STRING ," +
                " split_total_amount STRING ," +
                " split_activity_amount STRING ," +
                " split_coupon_amount STRING ," +
                " create_time TIMESTAMP(0) ," +
                "  `offset` BIGINT METADATA VIRTUAL  , " +
                "  WATERMARK FOR create_time as create_time - INTERVAL '0.001' SECOND " +
                SqlUtil.getKafkaSourceConfigSql(GmallConstant.TOPIC_DWD_TRADE_ORDER_DETAIL,GmallConstant.DWS_TRADE_SKU_ORDER_WINDOW);

        tableEnv.executeSql(odSql);

        /*
            2.由于dwd_order_detail 会出现同一个详情(order_detail_id)有多条数据的情况，因此要避免重复计算。

                解决方法一:
                            0. 先把sku_id 为null的过滤掉(省略)
                            1. 在窗口中，只对sku_id，order_detail_id 进行聚合
                              offset=5, id": "14245187",   sku_id=4, "province_id": null,
                              offset=6, id": "14245187",
                              offset=7, id": "14245187",   sku_id=4, "province_id": 1,
                            2. 采取 avg(split_total_amount)进行聚合。

                              offset=5, id": "14245187",   sku_id=4, split_total_amount = 10, "province_id": null,
                              offset=7, id": "14245187",   sku_id=4,split_total_amount = 10, "province_id": 1,
                            3. 还需要二次聚合
                                    在flink中二次聚合
                                        select  window_start,window_end,sku_id,sum(split_total_amount)  from tmp group by window_start,window_end,sku_id
                                        如何保证flink程序挂掉，Doris的结果不重复？
                                                Doris中value列选择 REPLACE
                                                可以不开启2PC提交。幂等输出。

                                    在Doris中二次聚合
                                        表模型必须是 aggregate，value列，要选择SUM类型

                                        如何保证flink程序挂掉，Doris的结果不重复？
                                                开启2PC提交。

                解决方法二:
                             0. 先把sku_id 为null的过滤掉
                             2. 在开窗前，对每条数据计算一个rn，按照时间(offset)升序排序

                              offset=5, id": "14245187",   sku_id=4, "province_id": null, rn = 1
                              offset=6, id": "14245187",                                   rn=2
                              offset=7, id": "14245187",   sku_id=4, "province_id": 1,      rn = 3
                            3.先过滤rn = 1 ,在窗口中，只对sku_id 进行聚合
                                offset=5, id": "14245187",   sku_id=4, "province_id": null, rn = 1
                                窗口中只需要一次聚合:  sum(split_total_amount)
         */

        //2. 对数据按照offset进行升序排序
        String rankSql = " select " +
                " * ," +
                " row_number() over(partition by id order by `offset` asc ) rn " +
                " from t1 " ;

        tableEnv.createTemporaryView("t2",tableEnv.sqlQuery(rankSql));
        tableEnv.createTemporaryView("t3",tableEnv.sqlQuery(" select * from t2 where rn = 1"));

        /*
            3.开窗计算
                报错: StreamPhysicalWindowAggregate doesn't support consuming update and delete changes which is produced by node Rank
                    只要使用了排名函数，不管排名函数是否会造成数据的更新和删除这些撤回操作，都不执行TVF窗口的计算。

                    从flink1.14开始，flink的TVF窗口，不支持处理排名后的数据。
                    flink的Group window(老的窗口api)支持处理排名后的数据。

                    优先推荐使用TVF窗口:   TABLE( TUMBLE|HOP XXXX )

                    老的API，group window:  session窗口，滚动和滑动

               解决方案：
                        使用老的api，group window解决。
                        TUMBLE(time_attr, interval): 开启一个滚动窗口
                        TUMBLE_START(time_attr, interval)： 滚动窗口的起始时间
                        TUMBLE_END(time_attr, interval)： 滚动窗口的结束时间
                            按照窗口进行分组。

         */
        String tumbleSql = " SELECT" +
                "  window_start stt," +
                "  window_end edt," +
                "  sku_id, " +
                "  TO_DATE(DATE_FORMAT(window_start,'yyyy-MM-dd')) cur_date," +
                "  sum( cast(sku_num as INT ) ) sku_num ," +
                "  sum( cast(split_activity_amount as decimal(16,2) ) ) activity_reduce_amount  ," +
                "  sum( cast(split_coupon_amount as decimal(16,2)) ) coupon_reduce_amount ," +
                "  sum( cast(split_total_amount as decimal(16,2)) ) order_amount ," +
                "   PROCTIME() pt " +
                "  FROM TABLE(" +
                "    TUMBLE(TABLE t3, DESCRIPTOR(create_time), INTERVAL '5' second )" +
                "   )" +
                "  GROUP BY window_start, window_end , sku_id ";

        String groupTumbleSql = " select " +
                "  TUMBLE_START(create_time, INTERVAL '5' second)  stt," +
                "  TUMBLE_END(create_time, INTERVAL '5' second)  edt," +
                "  sku_id," +
                "  sum( cast(sku_num as INT ) ) sku_num ," +
                "  sum( cast(split_activity_amount as decimal(16,2) ) ) activity_reduce_amount  ," +
                "  sum( cast(split_coupon_amount as decimal(16,2)) ) coupon_reduce_amount ," +
                "  sum( cast(split_total_amount as decimal(16,2)) ) order_amount ," +
                "  PROCTIME() pt " +
                " from t3 " +
                " group by TUMBLE(create_time, INTERVAL '5' second), sku_id";

        tableEnv.createTemporaryView("t4",tableEnv.sqlQuery(groupTumbleSql));

        //4.关联维度 使用look-upjoin
        String skuSql = " create table sku( " +
                "  id STRING ," +
                "  info Row<spu_id STRING,price STRING,sku_name STRING," +
                "   tm_id STRING, category3_id STRING > ," +
                "  PRIMARY KEY (id) NOT ENFORCED  " +
                SqlUtil.getHbaseSourceSql("cug_rt_gmall:dim_sku_info");

        String spuSql = " create table spu( " +
                "  id STRING ," +
                "  info Row<spu_name STRING> ," +
                "  PRIMARY KEY (id) NOT ENFORCED  " +
                SqlUtil.getHbaseSourceSql("cug_rt_gmall:dim_spu_info")
                ;

        String tmSql = " create table tm( " +
                "  id STRING ," +
                "  info Row<tm_name STRING> ," +
                "  PRIMARY KEY (id) NOT ENFORCED  " +
                SqlUtil.getHbaseSourceSql("cug_rt_gmall:dim_base_trademark")
                ;

        String c1Sql = " create table c1( " +
                "  id STRING ," +
                "  info Row<name STRING> ," +
                "  PRIMARY KEY (id) NOT ENFORCED  " +
                SqlUtil.getHbaseSourceSql("cug_rt_gmall:dim_base_category1")
                ;

        String c2Sql = " create table c2( " +
                "  id STRING ," +
                "  info Row<name STRING,category1_id STRING> ," +
                "  PRIMARY KEY (id) NOT ENFORCED  " +
                SqlUtil.getHbaseSourceSql("cug_rt_gmall:dim_base_category2")
                ;

        String c3Sql = " create table c3( " +
                "  id STRING ," +
                "  info Row<name STRING,category2_id STRING> ," +
                "  PRIMARY KEY (id) NOT ENFORCED  " +
                SqlUtil.getHbaseSourceSql("cug_rt_gmall:dim_base_category3")
                ;

        tableEnv.executeSql(skuSql);
        tableEnv.executeSql(spuSql);
        tableEnv.executeSql(tmSql);
        tableEnv.executeSql(c1Sql);
        tableEnv.executeSql(c2Sql);
        tableEnv.executeSql(c3Sql);

        String joinSql = " select " +
                " `stt`                 ," +
                "`edt`                  ," +
                " TO_DATE(DATE_FORMAT(stt,'yyyy-MM-dd')) `cur_date`             ," +
                " cast(tm.id as SMALLINT) `trademark_id`         ," +
                " tm.info.tm_name `trademark_name`       ," +
                " cast(c1.id as SMALLINT) `category1_id`         ," +
                " c1.info.name `category1_name`       ," +
                " cast(c2.id as SMALLINT) `category2_id`         ," +
                " c2.info.name `category2_name`       ," +
                " cast(c3.id as SMALLINT) `category3_id`         ," +
                " c3.info.name `category3_name`       ," +
                " cast(`sku_id` as INT)               ," +
                " sku.info.sku_name `sku_name`             ," +
                " cast(spu.id as INT) `spu_id`               ," +
                " spu.info.spu_name `spu_name`             ," +
                " sku_num * cast(sku.info.price as decimal(16,2)) `original_amount`      ," +
                "`activity_reduce_amount` ," +
                "`coupon_reduce_amount` ," +
                "`order_amount`         " +
                " from t4 " +
                " left join sku FOR SYSTEM_TIME AS OF t4.pt " +
                " ON t4.sku_id = sku.id " +
                " left join spu FOR SYSTEM_TIME AS OF t4.pt " +
                " ON sku.info.spu_id = spu.id " +
                " left join c3 FOR SYSTEM_TIME AS OF t4.pt " +
                " ON sku.info.category3_id = c3.id " +
                " left join c2 FOR SYSTEM_TIME AS OF t4.pt " +
                " ON c3.info.category2_id = c2.id " +
                " left join c1 FOR SYSTEM_TIME AS OF t4.pt " +
                " ON c2.info.category1_id = c1.id " +
                " left join tm FOR SYSTEM_TIME AS OF t4.pt " +
                " ON sku.info.tm_id = tm.id " ;

        //5.创建sink表
        String sinkSql = " create table t5 (" +
                "  `stt`            TIMESTAMP,       " +
                " `edt`                TIMESTAMP,   " +
                " `cur_date`         DATE,     " +
                " `trademark_id`     SMALLINT,     " +
                " `trademark_name`     STRING,   " +
                " `category1_id`      SMALLINT,    " +
                " `category1_name`     STRING,   " +
                " `category2_id`        SMALLINT,  " +
                " `category2_name`    STRING,    " +
                " `category3_id`      SMALLINT,    " +
                " `category3_name`    STRING,    " +
                " `sku_id`           INT,      " +
                " `sku_name`         STRING,     " +
                " `spu_id`          INT,      " +
                " `spu_name`        STRING,      " +
                " `original_amount`   DECIMAL(16,2),    " +
                " `activity_reduce_amount` DECIMAL(16,2)," +
                " `coupon_reduce_amount`  DECIMAL(16,2), " +
                " `order_amount`          DECIMAL(16,2) "+ SqlUtil.getDorisSinkSql("cug_rt_gmall.dws_trade_sku_order_window");

        tableEnv.executeSql(sinkSql);
        tableEnv.executeSql("insert into t5 "+ joinSql);





    }
}
