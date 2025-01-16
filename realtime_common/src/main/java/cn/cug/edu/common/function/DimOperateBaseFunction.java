package cn.cug.edu.common.function;


import cn.cug.edu.common.pojo.TableProcess;
import cn.cug.edu.common.util.ConfigUtil;
import cn.cug.edu.common.util.HbaseUtil;
import cn.cug.edu.common.util.JdbcUtil;
import cn.cug.edu.common.util.RedisUtil;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.functions.AbstractRichFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import redis.clients.jedis.Jedis;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Created by Smexy on 2024/1/9
 *
 *
RedisValue的类型:
    单值: 1:1  String,Hash
    多值: 1:N Set,Zset,List,Hash

维度数据在HBase和Redis中存储的模式:
 HBase的模式是表格，每一条数据都是表中的一行。 Table.get(new Get("id值"))；
 Redis的模式的是K-V对，每一条数据是以K-V对形式存储在Redis。

维度数据缓存在Redis中，value的选择:
    场景： 使用一个外键，到redis中查询整个维度信息
  1.目标是以表为粒度。
        典型的1：N的实体关系。1张表中有N条数据。
        K：表名
        V：
          Set[ {1},{2},{3}   ]， smembers k 只能取出整个表(set集合)，需要遍历，获取某个具体的维度
          List[ {1},{2},{3}   ], lrange k 只能取出整个表(List集合)，需要遍历，获取某个具体的维度
          Hash{1:{1},2:{2}   },  hget k ,1, 获取某个具体的维度，无需遍历。

        选择Hash。  有多少张表，就有多少个K-V，K的数量少，方便管理。 时间复杂度 O(1)



  2.目标是以一条数据为粒度。
        典型的1：1实体关系。一行作为一个K-V。
        K：表名+主键
        V：String(只读)
            Get k

        选择String。 有多少条维度数据，就有多少个K-V。K的数量多，不太方便管理，可以精细化管理。时间复杂度 O(1)
        可以设置value的ttl(过期时间)



 */
/*
    抽取操作redis和hbase的公共代码:
        1.创建hbase和redis的客户端
            redis: Jedis
            hbase:  Table
        2.提供读写hbase和redis的公共方法
 */
public class DimOperateBaseFunction extends AbstractRichFunction
{

    protected Jedis jedis;
    //每个Task所使用的函数对象，有唯一的Map，不是共享的
    protected Map<String, Table> tableMap = new HashMap<>();

    @Override
    public void open(Configuration parameters) throws Exception {
        jedis = RedisUtil.getJedis();
        //把要关联的hbase中的维度表的Table对象提前创建好
        List<TableProcess> result = JdbcUtil.queryList("select * from table_process where sink_type = 'DIM' ", TableProcess.class);
        //通过配置查询hbase中使用的业务库的名字
        String namespace = ConfigUtil.getString("HBASE_NAMESPACE");
        for (TableProcess tableProcess : result) {
            tableMap.put(tableProcess.getSourceTable(), HbaseUtil.getTable(namespace,tableProcess.getSinkTable()));
        }
    }

    @Override
    public void close() throws Exception {
        Set<Map.Entry<String, Table>> entries = tableMap.entrySet();
        for (Map.Entry<String, Table> entry : entries) {
            entry.getValue().close();
        }
        RedisUtil.close(jedis);
    }

    //读取redis的维度信息
    protected String getStringFromRedis(String table,String id){
        return jedis.get(getRediskey(table,id));
    }

    //向redis中写String的维度信息
    protected void setStringToRedis(String table,String id,String v){
         jedis.setex(getRediskey(table,id),ConfigUtil.getInt("JEDIS_STRING_TTL"),v);
    }

       /*
        向hbase中写维度，封装维度为Put
            一个Put代表对一行的增或改操作。
                一个Put中封装多个Cell(一列的一个版本)
     */
       protected Put putData(String rowkey, String sinkFamily, JSONObject data){
        Put put = new Put(Bytes.toBytes(rowkey));
        Set<String> fieldName = data.keySet();
        for (String field : fieldName) {
            put.addColumn(
                Bytes.toBytes(sinkFamily),
                Bytes.toBytes(field),
                //业务数据中某些字段的值，可能是null
                Bytes.toBytes(data.getString(field)==null?"null":data.getString(field))
            );
        }
        return put;
    }

    /*
        向hbase中读维度，把一行维度数据封装为一个JSONObject
     */
    protected JSONObject getDataFromHBase(Table t,String rowkey) throws IOException {

        JSONObject jsonObject = new JSONObject();

        Get get = new Get(Bytes.toBytes(rowkey));

        Result result = t.get(get);

        Cell[] cells = result.rawCells();
        for (Cell cell : cells) {
            //cell代表一列的一个版本。需要列名，列值
            String columnName = Bytes.toString(CellUtil.cloneQualifier(cell));
            String columnValue = Bytes.toString(CellUtil.cloneValue(cell));
            jsonObject.put(columnName,columnValue);
        }

        return jsonObject;

    }

    protected String getRediskey(String table,String id){
        return table + ":" +id;
    }
}
