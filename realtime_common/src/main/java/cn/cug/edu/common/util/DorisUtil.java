package cn.cug.edu.common.util;

import org.apache.doris.flink.cfg.DorisExecutionOptions;
import org.apache.doris.flink.cfg.DorisOptions;
import org.apache.doris.flink.cfg.DorisReadOptions;
import org.apache.doris.flink.sink.DorisSink;
import org.apache.doris.flink.sink.writer.serializer.SimpleStringSerializer;

/**
 * author song
 * date 2025-01-13 16:23
 * Desc
 */
public class DorisUtil {

    public static DorisSink<String> getDorisSink(String table){
        return  new DorisSink<>(
                DorisOptions.builder()
                        .setFenodes(ConfigUtil.getString("DORIS_FE"))
                        .setTableIdentifier(table)
                        .setUsername(ConfigUtil.getString("DORIS_USER"))
                        .setPassword(ConfigUtil.getString("DORIS_PASSWORD"))
                        .build(),
                DorisReadOptions.builder().build(),
                // 不对默认参数做任何设置 DorisExecutionOptions.defaults()
                DorisExecutionOptions
                        .builderDefaults()
                        .disable2PC()
                        .build(),
                new SimpleStringSerializer()
        );
    }
}
