package cn.cug.edu.common.function;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.PropertyNamingStrategy;
import com.alibaba.fastjson.serializer.SerializeConfig;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;

/**
 * author song
 * date 2025-01-13 16:28
 * Desc
 */
public class DorisMapFunction<T> extends RichMapFunction<T,String> {
    SerializeConfig serializeConfig ;
    @Override
    public void open(Configuration parameters) throws Exception {
        serializeConfig = new SerializeConfig();
        serializeConfig.setPropertyNamingStrategy(PropertyNamingStrategy.SnakeCase);
    }

    @Override
    public String map(T value) throws Exception {
        return  JSON.toJSONString(value,serializeConfig);
    }
}
