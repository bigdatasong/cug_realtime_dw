package cn.cug.edu.common.util;

import cn.cug.edu.common.constant.GmallConstant;

/**
 * author song
 * date 2024-08-16 21:44
 * Desc 关于连接器的一些工具类的方法
 */
public class SqlUtil {

    //定义一个方法实现使用sql连接kafka的source时的连接器参数
    public static String getKafkaSourceConfigSql(String topic,String groupId){
        String sql = " )WITH (" +
                " 'connector' = 'kafka'," +
                " 'topic' = '%s'," +
                " 'properties.bootstrap.servers' = '%s'," +
                " 'properties.group.id' = '%s'," +
                " 'scan.startup.mode' = 'earliest-offset', " +
                " 'json.fail-on-missing-field' = 'false', " +
                " 'json.ignore-parse-errors' = 'true', " +
                " 'format' = 'json'" +
                ")";

        //凡是需要格式话的参数都需要通过format进行格式化，对于topic需要当作方法参数来传递进去
        //以及这个消费者组id
        String kafkaBrokers = String.format(sql, topic, ConfigUtil.getString("KAFKA_BROKERS"), groupId);

        return kafkaBrokers;
    }

    // 普通kafka写入
    public static String getKafkaSinkSql(String topic){
        String sql =  " )with( " +
                " 'connector' = 'kafka', " +
                " 'topic' = '%s'," +
                "  'properties.bootstrap.servers' = '%s'," +
                "  'format' = 'json' " +
                ")";

        return String.format(sql,topic,ConfigUtil.getString("KAFKA_BROKERS"));
    }

    // upset kafka写入
    public static String getUpsertKafkaSinkSql(String topic){
        String sql =  " )with( " +
                " 'connector' = 'upsert-kafka', " +
                " 'topic' = '%s'," +
                "  'properties.bootstrap.servers' = '%s'," +
                "   'key.format' = 'json'," +
                "  'value.format' = 'json' " +
                ")";

        return String.format(sql,topic,ConfigUtil.getString("KAFKA_BROKERS"));
    }



    //实现对habse连接器参数的返回
    //对于连接器的参数的话没什么好解释的 首先就是版本要选对 因为hbase的元数据存放在zk中所以只需要zk的地址即可
    //然后就是hbase中的表名 注意这里需要给库名加表名 后面lookup join需要的参数 可以直接复制。
    public static String getHbaseSourceSql(String habseTableName){
        String sql = " )with('connector' = 'hbase-2.2'," +
                " 'zookeeper.quorum' = '%s' ," +
                " 'table-name' = '%s' ," +
                " 'lookup.async' = 'true' ," +
                " 'lookup.cache' = 'PARTIAL' ," +
                " 'lookup.partial-cache.max-rows' = '500' ," +
                " 'lookup.partial-cache.expire-after-write' = '1d' ," +
                " 'lookup.partial-cache.cache-missing-key' = 'true')";

        //需要的两个参数需要格式化 其中zk的地址可以直接从配置文件中读取，对于habse的表名可以从参数中传递 ，因为每个表名是不一样的.
        String hbaseZk = String.format(sql, ConfigUtil.getString("HBASE_ZK"), habseTableName);

        return hbaseZk;
    }

    //定义sql方式写出到doris的连接器参数 方法
    public static String getDorisSinkSql(String table){
        String sql =  " )WITH (" +
                "      'connector' = 'doris'," +
                "      'fenodes' = '%s'," +
                "      'table.identifier' = '%s'," +
                "      'username' = '%s'," +
                "      'password' = '%s'," +
                "      'sink.properties.format' = 'json', " +
                "      'sink.properties.read_json_by_line' = 'true', " +
                "      'sink.buffer-count' = '100', " +
                "      'sink.buffer-flush.interval' = '1s', " +
                "      'sink.enable-2pc' = 'false' " +   // 测试阶段可以关闭两阶段提交,方便测试
                ")";

        return String.format(sql,ConfigUtil.getString("DORIS_FE"),table,
                ConfigUtil.getString("DORIS_USER"),
                ConfigUtil.getString("DORIS_PASSWORD"));
    }


}
