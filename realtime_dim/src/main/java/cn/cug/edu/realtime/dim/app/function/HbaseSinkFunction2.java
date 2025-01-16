package cn.cug.edu.realtime.dim.app.function;

import cn.cug.edu.common.function.DimOperateBaseFunction;
import cn.cug.edu.common.pojo.TableProcess;
import cn.cug.edu.common.util.ConfigUtil;
import cn.cug.edu.common.util.HbaseUtil;
import com.alibaba.fastjson.JSONObject;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * author song
 * date 2025-01-16 18:36
 * Desc
 */
@Slf4j
public class HbaseSinkFunction2 extends DimOperateBaseFunction implements SinkFunction<Tuple2<JSONObject, TableProcess>> {


    //对每条业务数据进行写出
    @Override
    public void invoke(Tuple2<JSONObject, TableProcess> value, Context context) throws Exception {
        JSONObject data = value.f0;
        TableProcess config = value.f1;
        //从配置中获取信息
        String table = config.getSourceTable();
        //指原始数据中，哪个字段作为rowkey
        String sinkRowKey = config.getSinkRowKey();
        //根据字段取出rowkey的值,主键的值
        String rowkey = data.getString(sinkRowKey);
        String sinkFamily = config.getSinkFamily();
        //获取当前数据的操作类型
        String op_type = data.getString("op_type");
        //获取要写出的Table对象
        Table t = tableMap.get(table);
        //t可能会由于，当前处理的维度表是Job启动后，又增加的配置信息，而为null
        if(t == null){
            //手动创建
            t  = HbaseUtil.getTable(ConfigUtil.getString("HBASE_NAMESPACE"), config.getSinkTable());
            tableMap.put(table,t);
        }
        log.warn("取出:"+t.toString());
        //执行写出
        if ("delete".equals(op_type)){
            //从hbase维度表中删除这条记录
            t.delete(new Delete(Bytes.toBytes(rowkey)));
            //从redis中删除这条数据
            jedis.del(getRediskey(table,rowkey));
        }else {
            //向hbase维度表中写入这条记录
            Put put = putData(rowkey, sinkFamily, data);
            t.put(put);
            //向redis缓存中写入记录
            setStringToRedis(table,rowkey,data.toJSONString());
        }

    }
}
