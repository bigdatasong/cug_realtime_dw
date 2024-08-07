package cn.cug.edu.realtime.dim.app;

import cn.cug.edu.common.base.BaseDataStreamApp;
import cn.cug.edu.common.pojo.TableProcess;
import cn.cug.edu.common.util.ConfigUtil;
import cn.cug.edu.common.util.HbaseUtil;
import cn.cug.edu.common.util.MysqlUtil;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.google.common.collect.Sets;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.hadoop.hbase.client.Admin;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * author song
 * date 2024-08-03 21:37
 * Desc dimapp 用于继承baseapp 来实现job操作
 */
@Slf4j
public class DimApp extends BaseDataStreamApp {

    //每一个job必须要有一个main方法来实现job的操作
    public static void main(String[] args) {
        new DimApp().start(10001,4,"dim_app_0803", ConfigUtil.getString("TOPIC_ODS_DB"));
    }


    @Override
    protected void hadle(StreamExecutionEnvironment env, DataStreamSource<String> kfSource) {

        //先简单的输出
        kfSource.print();

        // 先做一个简单的etl
        SingleOutputStreamOperator<String> etlData = etl(kfSource);

        //开始动态分流 步骤：先去在mysql库中创建库和配置表 然后再用cdc的方式去读取这个配置，形成配置流，然后在和业务数据流进行
        //connect 将配置流以广播的形式来广播配置

        //定义一个方法得到配置流
        SingleOutputStreamOperator<TableProcess> dimConfig = getDimConfig(env);

        // 接下来就可以将两个流连接在一块了，但是在连接之前，我们需要先根据配置流来把表创建好，其实这个操作可以在刚刚那个map中创建，拆开再来一次map也是可以的

        // 因为配置流中的配置表就是需要和habse中表对应的，并且op的不同来执行对habse的表来操作
        //定义一个方法实现根据配置流来对hbase表进行创建
        SingleOutputStreamOperator<TableProcess> dimConfighbaseTable = createHbaseTable(dimConfig);
        // 调试的时候可以打印一下 habsetable.print();

        // 将两个流进行连接
        SingleOutputStreamOperator<Tuple2<JSONObject, TableProcess>> tuple2SingleOutputStreamOperator = connectDim(etlData, dimConfighbaseTable);

        //调试的时候也可以将其打印
        tuple2SingleOutputStreamOperator.print();

    }


    /**
     *  在dim层之前实现对数据的etl
     *   首先必须是json数据 在此前提下 生成的maxwell的数据格式中
     *     1.库必须是实时数仓的数据库，因为maxwell可以监听多个数据库
     *     2. 必须有表名
     *     3. 必须有ts字段，
     *     4. type类型 是根据maxwell来决定的，必须是insert update delete ，以及bootstrap-insert
     *          因为maxwell监听的是对数据操作的日志，所以肯定是这个几种类型，delete类型也是需要的，如果是对表进行删除的话，假设
     *          删除的是维度表，那么也需要后续操作的，另外在bootstrap-insert 也是一定的，因为在全量同步时，type的类型中只有bootstrap-insert
     *          中data有数据，其他两个类型只是开始和结束的标识
     *     5. 还有就是data中{} 必须要有数据
     * @param kfSource
     */
    private SingleOutputStreamOperator<String> etl(DataStreamSource<String> kfSource) {
        //通过fastjson工具的来解析json字符串，如果是json格式的字符串 就会解析成功成json对象，另外就是解析成json对象也方便对数据进行提取和
        // 操作，比直接流中是json字符串好操作，当然这里只是etl 先不改变流中的数据格式

        // 定义一个set集合 用来存放type的指定类型
        HashSet<String> typeSet = Sets.newHashSet("insert", "update", "delete", "bootstrap-insert");
        SingleOutputStreamOperator<String> filter = kfSource.filter(
                jsonStr -> {
                    try {
                        JSONObject jsonObject = JSON.parseObject(jsonStr);
                        // 在这里说明就是json格式的数据了
                        //提取需要判断的数据
                        String database = jsonObject.getString("database");
                        String table = jsonObject.getString("table");
                        String type = jsonObject.getString("type");
                        String ts = jsonObject.getString("ts");
                        String data = jsonObject.getString("data");
                        // 库必须是cug_rt_gmall， table type ts 以及data不能为空 并且type必须是指定类型
                        //data 的长度必须> 2 以上都是且的关系，只要有一个不满足就需要过滤掉
                        // 另外type必须是指定类型 所以可以用一个set集合来定义好 类型
                        return ConfigUtil.getString("BUSI_DATABASE").equals(database)
                                &&
                                // isnoneblank 表示的是可变参数中 只要有一个为null 空白字符 或者 ''
                                // null 和 '' (表示的是空串的意思) '' 和 null的区别在于 null 表示啥也没有，长度为0 而'' 表示的是
                                // 只有‘’ 里面的长度为0
                                StringUtils.isNoneBlank(table, type, ts, data)
                                &&
                                typeSet.contains(type)
                                &&
                                data.length() > 2;

                    } catch (Exception e) {
                        //这里说明不是json格式数据 可以给一个提示，并且我们只是过滤 所以需要过滤掉不是json格式的数据
                        //所以可以不抛异常 而是给出日志提示 并且返回false
                        //在类中导入slf4j 注解 这个注解是lombook 给的 他会提供一个log对象，直接调用他的warn方法即可
                        log.warn(jsonStr + "是非法的json格式");
                        return false;
                    }
                }
        );

        return filter;

    }

    // 得到配置流  并且我们知道 如果直接得到是一个json字符串 不是很好对数据进行操作 ，可以将其转为tableprocess对象
    private SingleOutputStreamOperator<TableProcess> getDimConfig(StreamExecutionEnvironment env) {


        // 在使用lambda表达式时 需要显示的返回值的泛型
        SingleOutputStreamOperator<TableProcess> map = env.fromSource(MysqlUtil.getMysqlSource(), WatermarkStrategy.noWatermarks(), "config")
                .setParallelism(1)
                // 在此之前调式的时候为了知道cdc读取mysql的数据格式 ，可以先print一下得到数据格式来更好的进行封装
                .map(jsonStr -> {

                    // 封装tableprocess 在cdc的数据格式中 数据根据op值不同，存放的位置也不同，
                    // 因为时json字符串 可以先变成jsonobject对象方便取数据
                    JSONObject jsonObject = JSON.parseObject(jsonStr);
                    //获取op 目的是 bean中有op字段需要封装，另外根据op不同，其真正的数据也在不同的位置
                    String opType = jsonObject.getString("op");
                    // 如果op 是r表示读的是快照数据，before中没有值，after有值，应该取after的值
                    // 如果op 是 u 和 c 都应该取after的值， 如果是d before有值， after没有值
                    // 综上来看应该如果是d就取before的值，如果是其他的就是取after值
                    TableProcess t = null;
                    if ("d".equals(opType)) {
                        String before = jsonObject.getString("before");
                        //封装tableprocess 提前声明一个变量
                        t = JSON.parseObject(before, TableProcess.class);
                    } else {
                        String after = jsonObject.getString("after");
                        t = JSON.parseObject(after, TableProcess.class);

                    }
                    // 都需要封装op
                    t.setOp(opType);
                    // 因为是map所以需要一个返回值
                    return t;
                }).setParallelism(1);

        return map;
    }


    //根据配置流创建habse表的创建
    private  SingleOutputStreamOperator<TableProcess> createHbaseTable(SingleOutputStreamOperator<TableProcess> dimConfig) {

        // 流中一个tableproces应该对应hasbse的表 返回值应该还是tableprocess 只是在这个流中创建表而已，创建表需要连接
        // 最好是使用带有生命周期的方法
        SingleOutputStreamOperator<TableProcess> map = dimConfig.map(new RichMapFunction<TableProcess, TableProcess>() {

            private Admin admin;

            @Override
            public void open(Configuration parameters) throws Exception {
                // 对表的操作应该得到admin
                admin = HbaseUtil.getAdmin();
            }

            @Override
            public TableProcess map(TableProcess tableProcess) throws Exception {

                // 根据op的类型来判断是否创建表 或者删除表
                //如果op是r 说明需要创建表 如果是u 需要先删除对应的表，再创建表，如果是c 需要创建表，如果是d 就需要删除表
                String opType = tableProcess.getOp();
                String sinkFamily = tableProcess.getSinkFamily();
                if ("d".equals(opType)) {
                    //调用方法执行删除表
                    // 库名在tableprocess中没有 应该从配置文件中读取
                    HbaseUtil.dropTable(admin, ConfigUtil.getString("HBASE_NAMESPACE"), tableProcess.getSinkTable());
                } else if ("u".equals(opType)) {
                    //先删除后创建
                    HbaseUtil.dropTable(admin, ConfigUtil.getString("HBASE_NAMESPACE"), tableProcess.getSinkTable());
                    HbaseUtil.createTable(admin, ConfigUtil.getString("HBASE_NAMESPACE"), sinkFamily);
                } else {
                    //其他两种情况都需要创建表
                    HbaseUtil.createTable(admin, ConfigUtil.getString("HBASE_NAMESPACE"), sinkFamily);
                }

                return tableProcess;
            }

            @Override
            public void close() throws Exception {
                //关闭连接
                admin.close();
            }
        });

        return map;
    }

    // 实现两个流的连接
    private SingleOutputStreamOperator<Tuple2<JSONObject, TableProcess>> connectDim(SingleOutputStreamOperator<String> etlData, SingleOutputStreamOperator<TableProcess> dimConfighbaseTable) {

        // 连接之前先将配置流变成一个广播流
        // 或者状态描述符 将来是要给业务数据流用的 那么如何确定k和v的类型呢，需要根据配置流的类型来确定 如果确定配置表中的唯一呢，可以根据配置表中的
        //source_table来唯一确定key value的就是一行数据 即tableprocess
        MapStateDescriptor<String, TableProcess> mapstatdescriptor = new MapStateDescriptor<>("configState", String.class, TableProcess.class);
        BroadcastStream<TableProcess> broadcast = dimConfighbaseTable.broadcast(mapstatdescriptor);

        //连接
        SingleOutputStreamOperator<Tuple2<JSONObject, TableProcess>> process = etlData.connect(broadcast).process(new BroadcastProcessFunction<String, TableProcess, Tuple2<JSONObject, TableProcess>>() {


            // 两个方法 第一个方法就是每来一条业务数据如果处理 第二个方法就是每来一条配置流数据如何处理 其中可以发现
            //两者方法的context是不一样的，前者的context只能读取状态，后者的context可以修改状态
            //两个方法执行顺序没有固定 谁来先执行 写代码的逻辑就是先操作下面的方法，将配置流的数据存放到状态中，然后业务流数据得到状态

            @Override
            public void processElement(String s, ReadOnlyContext readOnlyContext, Collector<Tuple2<JSONObject, TableProcess>> collector) throws Exception {

                // 连接的目的就是将业务数据流中配置数据和配置流一一对应，然后将配置数据写入hbase中，
                //如何对应呢，业务数据中有table字段可以知道是不是维度数据，还有就是在配置表中还有一个source_type字段 表示的是当前维度表的什么操作应该被记录
                //如果是all表示的是对这个维度表的所有操作都可以存入到hbase对应的表中
                //如果不是all 就需要根据这个字段，一般来说这个字段都是以。隔开的的字符串，那么就需要将其分开，然后根据原始数据中的type字段一一对应，还有一点需要注意的是
                //sourcetype字段可能会包含各种操作的元素，所以判断的时候只需要判断type是其中的某个就可以写入表中

                // 取出原始数据字段中type 目前是string 为了方便可以将其string转为jsonobject
                JSONObject jsonObject = JSON.parseObject(s);
                String type = jsonObject.getString("type");
                // 获取原始数据中的table
                String table = jsonObject.getString("table");
                // 获取状态
                ReadOnlyBroadcastState<String, TableProcess> broadcastState = readOnlyContext.getBroadcastState(mapstatdescriptor);
                TableProcess tableProcess = broadcastState.get(table);
                String sourceType = tableProcess.getSourceType();
                if (tableProcess != null) {
                    // 说明 这个是维度数据   还有就是data也是json 所以可以调用方法直接得到一个jsonobject
                    JSONObject data = jsonObject.getJSONObject("data");
                    if ("All".equals(sourceType)) {
                        //说明所有操作都可以
                        //向下游输出即可  向下游封装时 原始数据中其实真正的数据在data上，所以其实就把data封装成jsonobject为tuple2中key即可
                        collector.collect(Tuple2.of(jsonObject, tableProcess));
                    } else {
                        // 将sourcetype切分，然后将其和type来判断
                        String[] split = sourceType.split(",");
                        Set<String> setSourceType = Arrays.stream(split).collect(Collectors.toSet());
                        if (setSourceType.contains(type)) {
                            //说明这些操作可以 也可向下游输出
                            collector.collect(Tuple2.of(data, tableProcess));
                        }
                    }

                }
            }

            @Override
            public void processBroadcastElement(TableProcess tableProcess, Context context, Collector<Tuple2<JSONObject, TableProcess>> collector) throws Exception {

                // 配置流操作方法 先得到状态 然后需要考虑到配置表中操作和状态中同步，比如说我对配置表的操作是删除，那么此时我状态应该也是没有这条对应的配置数据的
                //先获取状态
                BroadcastState<String, TableProcess> broadcastState = context.getBroadcastState(mapstatdescriptor);
                String op = tableProcess.getOp();
                if ("d".equals(op)) {
                    //删除状态中对应的数据
                    broadcastState.remove(tableProcess.getSourceTable());
                } else {
                    broadcastState.put(tableProcess.getSourceTable(), tableProcess);
                }

            }

        });

        return process;
    }



}
