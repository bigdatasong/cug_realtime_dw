package cn.cug.edu.realtime.dim.app.function;

import cn.cug.edu.common.pojo.TableProcess;
import cn.cug.edu.common.util.ConfigUtil;
import cn.cug.edu.common.util.HbaseUtil;
import cn.cug.edu.common.util.JdbcUtil;
import cn.cug.edu.common.util.MysqlUtil;
import com.alibaba.fastjson.JSONObject;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;

/**
 * author song
 * date 2024-08-09 9:40
 * Desc 实现将dim层的数据写出到habse中 需要自定义sink 因为flink没有写出到hbase的官方sink
 *     使用flink的自定义sink的话，有sinkto(Sink x) addsink(sinkfuntion x)
 *     前者是新的api但是比较的麻烦 后者是老的api 实现比较简单，所以就使用addsink方法 就需要一个sinkfunction对象
 *
 *     写入数据库都需要创建连接 为保证一个task一个线程 一个连接 对表数据的crud的话就需要table对象
 *     table对象是线程不安全的 所以一个线程一个table对象所以需要声明周期方法
 *
 *
 */
public class HbaseSink extends RichSinkFunction<Tuple2<JSONObject, TableProcess>> {

    private HashMap<String, Table> tableHashMap = new HashMap<>(); //定义一个属性

    @Override
    public void open(Configuration parameters) throws Exception {
        //在open方法中完成table对象的创建
        //调用工具类需要库名和表名 库名可以从配置文件中获取 而因为open方法是在task创建的时候只执行一次
        //但是task创建的时候流的数据还没来 所以获取不到流中的数据，所以还是需要从mysql的表中读取出来 和上一步的操作是一样的
        //所以同样 先用一个map来存 从msyql配置表中读取的数据 但是这个map将来是要用到addsink中，所以key可以是sink_table value就是
        //对应的table对象

        List<TableProcess> tableProcesses = JdbcUtil.queryList("select * from rt_gmallConfig where sink_type = 'DIM' ", TableProcess.class);
        for (TableProcess tableProcess : tableProcesses) {



            tableHashMap.put(tableProcess.getSinkTable(),HbaseUtil.getTable(ConfigUtil.getString("CONFIG_DATABASE"),tableProcess.getSinkTable()));

        }

    }

    @Override
    public void close() throws Exception {
        //在close方法中将每个table对象进行关闭
        Collection<Table> tables = tableHashMap.values();
        for (Table table : tables) {
            table.close();
        }

    }

    @Override
    public void invoke(Tuple2<JSONObject, TableProcess> value, Context context) throws Exception {

        // invoke方法表示的是流中数据每来一条的操作过程 在这里我们就要实现数据写入到habse中了
        //Jsonobject中有一个optype字段可以不用写入
        //先准备数据
        JSONObject data = value.f0;
        TableProcess tab = value.f1;
        //写入habse就需要用到hbaseutill工具类中的封装好的返回的put方法 参数需要rowkey 列族 列名 以及列值
        String sinkRowKey = tab.getSinkRowKey(); //这个值他指的是原始数据中哪个字段作为rowkey 所以我们还需要获取该字段对应的值
        //才是要写入的rowkey值
        String rowkeyValue = data.getString(sinkRowKey);
        String sinkFamily = tab.getSinkFamily(); //这个是列族名
        //而对于具体的列值和列名 其实在data中就是要写入的字段以及字段值，所以可以不用在这里获取即可
        //从map中获取到对应的table对象执行put写入
        Table table = tableHashMap.get(tab.getSinkTable());
        //调用table的put方法 需要一个put对象 干脆定义一个方法来返回一个put 为什么不直接使用hbaseutil中的getput方法呢 是因为那个返回的put
        //只是一行中的一个cell，而我们是有多个列名 即一行多个cell 所以这里再定义一个方法实现返回一个put对象 一行的数据中多个列

        //为了使用stringapi这里方便起见还是获取到所有的列名
        String sinkColumns = tab.getSinkColumns();
        //然后还需要把data也传入进去
        Put put = createPut(rowkeyValue, sinkFamily, sinkColumns, data);

        //最后需要考虑的是 put表示的是对单行的新增或者修改 但是还有一种情况就是还有delete的情况所以需要编写一个delete的put来执行
        //先获取操作类型
        String opType = data.getString("op_type");
        if ("delete".equals(opType)){
            //执行delete的方式
            table.delete(new Delete(Bytes.toBytes(rowkeyValue)));
        }else {
            //执行put操作
            table.put(createPut(rowkeyValue,sinkFamily,sinkColumns,data));
        }

    }

    //实现put对象的返回
    private Put createPut(String rowkeyValue, String sinkFamily, String sinkColumns, JSONObject data) {
        Put put = new Put(Bytes.toBytes(rowkeyValue));

        //将一行中每一个列进行封装
        Arrays.stream(sinkColumns.split(",")).forEach(
                col -> {
                    //通过列名来获取data中的列值
                    //这里还需要注意点的就是 如果value为null的话 下面的tobytes方法就会报空指针异常所以 需要判断不为null
                    String value = data.getString(col);
                    if (value != null) {

                        put.addColumn(Bytes.toBytes(sinkFamily), Bytes.toBytes(col), Bytes.toBytes(value));
                    }
                }
        );

        return put;
    }
}
