package cn.cug.edu.dws.cartAdd.app;

import cn.cug.edu.common.base.BaseStreamTableApp;
import cn.cug.edu.common.constant.GmallConstant;
import cn.cug.edu.common.util.SqlUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * author song
 * date 2025-01-14 16:36
 * Desc
 */
public class DwsTradeCartAddUuWindow extends BaseStreamTableApp {

    public static void main(String[] args) {
        new DwsTradeCartAddUuWindow().start(
                13006,4, GmallConstant.DWS_TRADE_CART_ADD_UU_WINDOW
        );
    }
    @Override
    protected void handle(StreamTableEnvironment tableEnv, StreamExecutionEnvironment env) {

         /*
            1.从dwd_cart_add读取数据

         */
        String urSql = " create table t1(" +
                " ts BIGINT," +
                " user_id STRING," +
                " et as TO_TIMESTAMP_LTZ(ts,0) ," +
                " WATERMARK FOR et as et - INTERVAL '0.001' SECOND " +
                SqlUtil.getKafkaSourceConfigSql(GmallConstant.TOPIC_DWD_TRADE_CART_ADD,GmallConstant.DWS_TRADE_CART_ADD_UU_WINDOW);

        tableEnv.executeSql(urSql);

        //2.开窗计算
        String tumbleSql = " SELECT" +
                "  window_start," +
                "  window_end," +
                "  TO_DATE(DATE_FORMAT(window_start,'yyyy-MM-dd')) cur_date," +
                "  count(distinct user_id) cart_add_uu_ct " +
                "  FROM TABLE(" +
                "    TUMBLE(TABLE t1, DESCRIPTOR(et), INTERVAL '5' second )" +
                "   )" +
                "  GROUP BY window_start, window_end  ";

        //3.创建sink表
        String sinkSql = " create table t2 (" +
                "   `stt` TIMESTAMP ," +
                "   `edt` TIMESTAMP ," +
                "   `cur_date`  DATE ," +
                "   `cart_add_uu_ct`   BIGINT " +
                SqlUtil.getDorisSinkSql("cug_rt_gmall.dws_trade_cart_add_uu_window");

        tableEnv.executeSql(sinkSql);

        //5.写出
        tableEnv.executeSql("insert into t2 " + tumbleSql);



    }
}
