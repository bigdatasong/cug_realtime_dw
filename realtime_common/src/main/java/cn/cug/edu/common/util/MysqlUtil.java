package cn.cug.edu.common.util;

import com.mysql.cj.jdbc.MysqlDataSource;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;

/**
 * author song
 * date 2024-08-07 10:10
 * Desc  现在的需求场景就是通过cdc的方式去读取mysql
 *       所以可以通过工具类的方式来返回得到一个mysqlsource
 */
public class MysqlUtil {

    public static MySqlSource<String> getMysqlSource(){
       return MySqlSource.<String>builder()
                .hostname(ConfigUtil.getString("MYSQL_HOST"))
                .port(ConfigUtil.getInt("MYSQL_PORT"))
                .databaseList(ConfigUtil.getString("CONFIG_DATABASE")) // 设置捕获的数据库， 如果需要同步整个数据库，请将 tableList 设置为 ".*".
                .tableList(ConfigUtil.getString("CONFIG_DATABASE")+'.' + ConfigUtil.getString("CONFIG_TABLE")) // 设置捕获的表
                .username(ConfigUtil.getString("MYSQL_USER"))
                .password(ConfigUtil.getString("MYSQL_PASSWORD"))
                // 表示的是第一次读取时 读取快照，即配置表中原有的数据
                .startupOptions(StartupOptions.initial())
                .deserializer(new JsonDebeziumDeserializationSchema()) // 将 SourceRecord 转换为 JSON 字符串
                .build();


    }


}
