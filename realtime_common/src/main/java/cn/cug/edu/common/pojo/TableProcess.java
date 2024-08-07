package cn.cug.edu.common.pojo;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * author song
 * date 2024-08-07 10:00
 * Desc
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class TableProcess {

    // 来源表
    String sourceTable;
    // 来源操作类型
    String sourceType;
    // 输出表
    String sinkTable;
    // 输出类型 dwd | dim
    String sinkType;
    // 数据到 hbase 的列族
    String sinkFamily;
    // 输出字段
    String sinkColumns;
    // sink到 hbase 的时候的主键字段
    String sinkRowKey;
    String op; // 配置表操作: c r u d 也即对配置表数据的crud
    // 因为当用cdc的方式去读取这个配置表时，如果说对这个配置表进行了一些变动时，cdc是能够捕捉到的，捕捉到后在cdc中会有特定的json格式
    //在json格式中就有这个op字段。这个op字段也是很重要的，如果说配置表的数据发生变动了，在后面形成的流也需要发生变动。做相应的处理
}
