package cn.cug.edu.dws.order.app;


import cn.cug.edu.common.base.BaseStreamTableApp;
import cn.cug.edu.common.constant.GmallConstant;
import cn.cug.edu.common.util.SqlUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * author song
 * date 2025-01-16 17:17
 * Desc
 */
public class DwsTradeOrderWindow extends BaseStreamTableApp {

    public static void main(String[] args) {
        new DwsTradeOrderWindow().start(
                13008,4, GmallConstant.DWS_TRADE_ORDER_WINDOW
        );
    }

    @Override
    protected void handle(StreamTableEnvironment tableEnv, StreamExecutionEnvironment env) {


        /*
            1.从dwd_order_detail(有撤回操作)读取数据

            create_time是2023-12-11 15:34:58类型，可以直接使用 TIMESTAMP类型映射
         */
        String detailSql = " create table t1(" +
                "  id STRING, " +
                " create_time TIMESTAMP(0)," +
                " user_id STRING," +
                " ts BIGINT ," +
                " et as TO_TIMESTAMP_LTZ(ts,0) ," +
                " WATERMARK FOR et as et - INTERVAL '0.001' SECOND " +
                SqlUtil.getKafkaSourceConfigSql(GmallConstant.TOPIC_DWD_TRADE_ORDER_DETAIL,GmallConstant.DWS_TRADE_ORDER_WINDOW);

        tableEnv.executeSql(detailSql);

        /*
            2.从dwd_order_detail(有撤回操作)读取数据
                order_detail  left join order_info  补充user_id
                              left join order_detail_coupon
                              left join order_detail_activity

              -----------------------
                +I order_detail_id = 3, user_id=null,coupon_id=null,activity_id=null
                -D order_detail_id = 3, user_id=null,coupon_id=null,activity_id=null
                +I order_detail_id = 3, user_id=1,coupon_id=null,activity_id=null
                -D order_detail_id = 3, user_id=1,coupon_id=null,activity_id=null
                +I order_detail_id = 3, user_id=1,coupon_id=2,activity_id=null

             -----------------------
                求是否是当日的 order_new_user_count，必须维护首次下单日期
                求是否是当日的 order_unique_user_count，必须维护末次下单日期
                    和DataStreamAPI不同，这里要维护的应该是日期+时间

             --------------------
                解释:  对数据的标准化处理的流程是不同的
                    DataStreamAPI
                        user_Id=1,order_date=2024-01-08 01:01:01,order_unique_user_count=1l,lastOrderDate=null
                        user_Id=1,order_date=2024-01-08 01:01:02,order_unique_user_count=0l,lastOrderDate=2024-01-08
                        user_Id=1,order_date=2024-01-08 01:01:03,order_unique_user_count=0l,lastOrderDate=2024-01-08
                        sum(1+0+0)

                    Sql:  统计order_unique_user_count, 维护今天的第一次下单时间
                           user_Id=1,order_date=2024-01-08 01:01:01,firstOrderDateTimeToDay=2024-01-08 01:01:01
                           user_Id=1,order_date=2024-01-08 01:01:02,firstOrderDateTimeToDay=2024-01-08 01:01:01
                           user_Id=1,order_date=2024-01-08 01:01:03,firstOrderDateTimeToDay=2024-01-08 01:01:01

                           count(dictinct if(order_date = firstOrderDateTimeToDay,user_Id,null))
                            去重的原因是，同一个用户的下单详情会有多条，由于dwd_order_detail有撤回

                    Sql:  统计order_new_user_count, 维护历史的第一次下单时间
                           user_Id=1,order_date=2024-01-08 01:01:01,firstOrderDateTimeHistory=2024-01-08 01:01:01
                           user_Id=1,order_date=2024-01-08 01:01:02,firstOrderDateTimeHistory=2024-01-08 01:01:01
                           user_Id=1,order_date=2024-01-08 01:01:03,firstOrderDateTimeHistory=2024-01-08 01:01:01
                           user_Id=1,order_date=2024-01-09 01:01:03,firstOrderDateTimeHistory=2024-01-08 01:01:01

                           count(distinct if(order_date = firstOrderDateTimeHistory,user_Id,null))



         */
        String firstOrderDateTimeToDaySql = " select" +
                " *, " +
                "  min(create_time) over( partition by user_id,date_format(create_time,'yyyy-MM-dd') order by et  ) firstOrderDateTimeToDay " +
                " from t1" +
                " where user_id is not null ";

        tableEnv.createTemporaryView("t2",tableEnv.sqlQuery(firstOrderDateTimeToDaySql));

        String firstOrderDateTimeHistorySql = " select" +
                "  *, " +
                "  min(create_time) over( partition by user_id order by et  ) firstOrderDateTimeHistory " +
                " from t1" +
                " where user_id is not null ";

        tableEnv.createTemporaryView("t3",tableEnv.sqlQuery(firstOrderDateTimeHistorySql));

        //3.开窗计算
        String tumbleSql1 = " SELECT" +
                "  window_start," +
                "  window_end," +
                "  TO_DATE(DATE_FORMAT(window_start,'yyyy-MM-dd')) cur_date," +
                "  count(distinct (if(firstOrderDateTimeToDay = create_time,user_id,cast(null as STRING) ) )) order_unique_user_count " +
                "  FROM TABLE(" +
                "    TUMBLE(TABLE t2, DESCRIPTOR(et), INTERVAL '5' second )" +
                "   )" +
                "  GROUP BY window_start, window_end  ";

        String tumbleSql2 = " SELECT" +
                "  window_start," +
                "  window_end," +
                "  TO_DATE(DATE_FORMAT(window_start,'yyyy-MM-dd')) cur_date," +
                "  count(distinct (if(firstOrderDateTimeHistory = create_time,user_id,cast(null as STRING) ) )) order_new_user_count " +
                "  FROM TABLE(" +
                "    TUMBLE(TABLE t3, DESCRIPTOR(et), INTERVAL '5' second )" +
                "   )" +
                "  GROUP BY window_start, window_end  ";

        tableEnv.createTemporaryView("t4",tableEnv.sqlQuery(tumbleSql1));
        tableEnv.createTemporaryView("t5",tableEnv.sqlQuery(tumbleSql2));

        //4.关联
        String joinSql = " select " +
                " t4.*, order_new_user_count" +
                " from t4 join t5 " +
                " on t4.window_start = t5.window_start " +
                " and  t4.window_end = t5.window_end ";



        //5.创建sink表
        String sinkSql = " create table t6 (" +
                "   `stt` TIMESTAMP ," +
                "   `edt` TIMESTAMP ," +
                "   `cur_date`  DATE ," +
                "   `order_unique_user_count`   BIGINT ," +
                "   `order_new_user_count`   BIGINT " +
                SqlUtil.getDorisSinkSql("cug_rt_gmall.dws_trade_order_window");

        tableEnv.executeSql(sinkSql);

        //5.写出
        tableEnv.executeSql("insert into t6 "+ joinSql);




    }
}
