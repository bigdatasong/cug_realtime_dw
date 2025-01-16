package cn.cug.edu.dwd.add.app;

import cn.cug.edu.common.base.BaseStreamTableApp;
import cn.cug.edu.common.constant.GmallConstant;
import cn.cug.edu.common.util.SqlUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * author song
 * date 2025-01-12 22:30
 * Desc
 */
public class DwdTradeCartAdd extends BaseStreamTableApp {

    public static void main(String[] args) {

        new DwdTradeCartAdd().start(11003,4, GmallConstant.TOPIC_DWD_TRADE_CART_ADD);

    }

    @Override
    protected void handle(StreamTableEnvironment tableEnv, StreamExecutionEnvironment env) {

        //1.读ods层的原始数据(ods_db),名字叫ods_db
        createOdsDB(tableEnv,GmallConstant.TOPIC_DWD_TRADE_CART_ADD);

        //2.过滤出 cart_info的insert和update操作的数据
        String cartInfoSql = " select " +
                "  data['id'] id, " +
                "  data['user_id'] user_id, " +
                "  data['sku_id'] sku_id, " +
                "  data['cart_price'] cart_price, " +
                "  data['sku_name'] sku_name, " +
                "  cast(data['sku_num'] as int) - cast(ifnull(`old`['sku_num'],'0') as int)  sku_num, " +
                "   ts   " +
                " from ods_db " +
                " where `database` = 'cug_rt_gmall' " +
                " and `table` = 'cart_info' " +
                " and (`type` = 'insert' or (`type` = 'update' and cast(data['sku_num'] as int) > cast(`old`['sku_num'] as int) )) ";


        //3.创建一个sink表
        String sinkSql = " create table " + GmallConstant.TOPIC_DWD_TRADE_CART_ADD +"(" +
                " id STRING, " +
                " user_id STRING, " +
                " sku_id STRING, " +
                " cart_price STRING, " +
                " sku_name STRING , " +
                "  sku_num INT , " +
                "   ts BIGINT " + SqlUtil.getKafkaSinkSql(GmallConstant.TOPIC_DWD_TRADE_CART_ADD);

        tableEnv.executeSql(sinkSql);

        //4.写出
        tableEnv.executeSql("insert into "+GmallConstant.TOPIC_DWD_TRADE_CART_ADD + cartInfoSql);



    }
}
