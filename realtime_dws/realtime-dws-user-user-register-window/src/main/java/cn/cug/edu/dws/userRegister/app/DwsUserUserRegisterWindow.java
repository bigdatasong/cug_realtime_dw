package cn.cug.edu.dws.userRegister.app;

import cn.cug.edu.common.base.BaseStreamTableApp;
import cn.cug.edu.common.constant.GmallConstant;
import cn.cug.edu.common.util.SqlUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * author song
 * date 2025-01-14 16:07
 * Desc
 */
public class DwsUserUserRegisterWindow extends BaseStreamTableApp {

    public static void main(String[] args) {
        new DwsUserUserRegisterWindow().start(
                13005,4, GmallConstant.DWS_USER_USER_REGISTER_WINDOW
        );
    }
    @Override
    protected void handle(StreamTableEnvironment tableEnv, StreamExecutionEnvironment env) {

         /*
            1.从dwd_user_register读取数据

            create_time是2023-12-11 15:34:58类型，可以直接使用 TIMESTAMP类型映射
         */
        String urSql = " create table t1(" +
                " create_time TIMESTAMP(0)," +
                " id STRING," +
                " WATERMARK FOR create_time as create_time - INTERVAL '0.0001' SECOND " +
                SqlUtil.getKafkaSourceConfigSql(GmallConstant.TOPIC_DWD_USER_REGISTER,GmallConstant.DWS_USER_USER_REGISTER_WINDOW);

        tableEnv.executeSql(urSql);

        //2.开窗计算
        String tumbleSql = " SELECT" +
                "  window_start," +
                "  window_end," +
                "  TO_DATE(DATE_FORMAT(window_start,'yyyy-MM-dd')) cur_date," +
                "  count(*) register_ct " +
                "  FROM TABLE(" +
                "    TUMBLE(TABLE t1, DESCRIPTOR(create_time), INTERVAL '5' second )" +
                "   )" +
                "  GROUP BY window_start, window_end  ";

        //3.创建sink表
        String sinkSql = " create table t2 (" +
                "   `stt` TIMESTAMP ," +
                "   `edt` TIMESTAMP ," +
                "   `cur_date`  DATE ," +
                "   `register_ct`   BIGINT " +
                SqlUtil.getDorisSinkSql("cug_rt_gmall.dws_user_user_register_window");

        tableEnv.executeSql(sinkSql);

        //env.sqlQuery(tumbleSql).execute().print();

        //5.写出
        tableEnv.executeSql("insert into t2 " + tumbleSql);



    }
}
