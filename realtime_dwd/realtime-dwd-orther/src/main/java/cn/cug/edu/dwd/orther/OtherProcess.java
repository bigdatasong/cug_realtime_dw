package cn.cug.edu.dwd.orther;

import cn.cug.edu.common.base.BaseDataStreamApp;
import cn.cug.edu.common.constant.GmallConstant;
import cn.cug.edu.common.pojo.TableProcess;
import cn.cug.edu.common.util.ConfigUtil;
import cn.cug.edu.common.util.JdbcUtil;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.producer.ProducerConfig;

import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.stream.Collectors;

/**
 * author song
 * date 2025-01-13 6:14
 * Desc
 */
@Slf4j
public class OtherProcess extends BaseDataStreamApp {

    public static void main(String[] args) {
        new OtherProcess().start(
                11009,4, "dwd_other_process", ConfigUtil.getString("TOPIC_ODS_DB")
        );
    }
    @Override
    protected void hadle(StreamExecutionEnvironment env, DataStreamSource<String> kfSource) {

        //1.etl
        SingleOutputStreamOperator<String> etlDs = etl(kfSource);
        //2.读取配置信息
        SingleOutputStreamOperator<TableProcess> configDs = getDwdConfig(env);
        //3.connect 事实和配置
        SingleOutputStreamOperator<Tuple2<JSONObject, TableProcess>> connectDS = connect(etlDs, configDs);
        //4.删除不需要的字段
        SingleOutputStreamOperator<Tuple2<JSONObject, TableProcess>> finalDs = dropFileds(connectDS);

        /*
            5.写出到kafka
                方式一： 提前分流，分别写出
                方法二： 不分流，写出时，生产者根据数据的配置，将数据发送到不同的kafka主题。
         */
        writeToKafka(finalDs);

    }

    private void writeToKafka(SingleOutputStreamOperator<Tuple2<JSONObject, TableProcess>> finalDs) {

        KafkaSink<Tuple2<JSONObject, TableProcess>> sink = KafkaSink
                .<Tuple2<JSONObject, TableProcess>>builder()
                .setBootstrapServers(ConfigUtil.getString("KAFKA_BROKERS"))
                .setRecordSerializer(
                        KafkaRecordSerializationSchema
                                .<Tuple2<JSONObject, TableProcess>>builder()
                                //通过这个方法，告知每条数据应该写出到哪个topic
                                .setTopicSelector(t -> t.f1.getSinkTable())
                                .setValueSerializationSchema(new SerializationSchema<Tuple2<JSONObject, TableProcess>>()
                                {
                                    //把数据中想写出的数据，转换为byte[]
                                    @Override
                                    public byte[] serialize(Tuple2<JSONObject, TableProcess> element) {
                                        return element.f0.toJSONString().getBytes(StandardCharsets.UTF_8);
                                    }
                                })
                                .build()
                )
                //必须设置为EOS
                .setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
                //开启了ck，基于2PC提交的事务写出时，可以给每个事务添加一个前缀
                .setTransactionalIdPrefix("song-" + "otherProcess")
                .setProperty(ProducerConfig.BATCH_SIZE_CONFIG, "1000")
                .setProperty(ProducerConfig.LINGER_MS_CONFIG, "1000")
                .setProperty(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG, 10 * 60000 + "")
                .build();

        finalDs.sinkTo(sink);

    }

    private SingleOutputStreamOperator<Tuple2<JSONObject, TableProcess>> dropFileds(SingleOutputStreamOperator<Tuple2<JSONObject, TableProcess>> connectedDs) {

        return connectedDs
                .map(new MapFunction<Tuple2<JSONObject, TableProcess>, Tuple2<JSONObject, TableProcess>>()
                {
                    @Override
                    public Tuple2<JSONObject, TableProcess> map(Tuple2<JSONObject, TableProcess> value) throws Exception {
                        //取出要过滤的业务数据
                        JSONObject originalData = value.f0;
                        //获取到要保留的字段
                        TableProcess config = value.f1;
                        Set<String> fileds = Arrays.stream(config.getSinkColumns().concat(",").concat("op_type").split(",")).collect(Collectors.toSet());
                        //使用要保留的字段，对原始的业务数据，进行过滤，只留下要保留的字段
                   /* JSONObject result = new JSONObject();
                    Set<Map.Entry<String, Object>> entries = originalData.entrySet();
                    for (Map.Entry<String, Object> entry : entries) {
                        if (fileds.contains(entry.getKey())){
                            result.put(entry.getKey(),entry.getValue());
                        }
                    }*/
                    /*
                        filterKeys(Map<K, V> unfiltered, Predicate<? super K> keyPredicate):
                            Map<K, V> unfiltered: 要过滤的Map
                            Predicate<? super K> 一个key的验证条件。把验证条件返回true的key留下来
                     */
                        value.f0 = new JSONObject(Maps.filterKeys(originalData, fileds::contains));
                        return value;
                    }
                });

    }

    private SingleOutputStreamOperator<Tuple2<JSONObject, TableProcess>> connect(SingleOutputStreamOperator<String> dwdDs, SingleOutputStreamOperator<TableProcess> configDs) {


        MapStateDescriptor<String, TableProcess> mapStateDescriptor = new MapStateDescriptor<>("configState", String.class, TableProcess.class);
        //把配置流制作为广播流
        BroadcastStream<TableProcess> broadcastStream = configDs.broadcast(mapStateDescriptor);

        return dwdDs
                .connect(broadcastStream)
                .process(new BroadcastProcessFunction<String, TableProcess, Tuple2<JSONObject, TableProcess>>()
                {

                    private String getKey(String sourceTable,String type){
                        return sourceTable+"_"+type;
                    }
                    private Map<String,TableProcess> configMap = new HashMap<>();
                    //在生命周期的方法中，不能操作管理状态
                    @Override
                    public void open(Configuration parameters) throws Exception {
                        //查询gmall2023_config.table_process的配置信息，存入集合中
                        List<TableProcess> result = JdbcUtil.queryList("select * from table_process where sink_type = 'DWD' ", TableProcess.class);
                        for (TableProcess tableProcess : result) {
                            //key: 选择源表名+op
                            configMap.put(getKey(tableProcess.getSourceTable(),tableProcess.getSourceType()),tableProcess);
                        }
                    }

                    //处理业务数据(事实+维度),写(增，删，改)入hbase的维度表中。通过maxwell采集的数据中的type来控制当前对hbase中维度表的数据进行增，删，改
                    @Override
                    public void processElement(String value, ReadOnlyContext ctx, Collector<Tuple2<JSONObject, TableProcess>> out) throws Exception {

                        ReadOnlyBroadcastState<String, TableProcess> broadcastState = ctx.getBroadcastState(mapStateDescriptor);

                        JSONObject jsonObject = JSON.parseObject(value);
                        //业务表的名字
                        String table = jsonObject.getString("table");
                        String type = jsonObject.getString("type");
                    /*
                        取出的v可能为null。
                            情形一：  value当前是事实表的数据，不是维度表的数据
                            情形二：  value当前是维度表的数据，但是广播状态中，还没有收到维度的配置信息
                     */
                        TableProcess v = broadcastState.get(getKey(table,type));

                        if (v == null){
                            v = configMap.get(getKey(table,type));
                            //log.warn(v+" 无法从广播状态中获取....");
                        }

                        //记录当前这条业务数据的操作方式
                        JSONObject data = jsonObject.getJSONObject("data");
                        data.put("op_type",type);

                        if (v != null) {
                            //说明当前数据是你要采集的维度数据。接下来需要再判断，当前记录的操作类型是否和source_type(要采集的类型)一致
                            String sourceType = v.getSourceType();
                            if ("ALL".equals(sourceType)) {
                                out.collect(Tuple2.of(data, v));
                            } else {
                                String[] ops = sourceType.split(",");
                                Set<String> opsets = Arrays.stream(ops).collect(Collectors.toSet());
                                if (opsets.contains(type)) {
                                    out.collect(Tuple2.of(data, v));
                                }
                            }
                        }
                    }

                    //处理配置，控制hbase中维度表的创建，删除，修改等  ，通过TableProcess的op来控制当前对hbase中维度表的增，删，改
                    @Override
                    public void processBroadcastElement(TableProcess value, Context ctx, Collector<Tuple2<JSONObject, TableProcess>> out) throws Exception {
                        //收到一条配置，放入广播状态。 需要随着 table_process这张表同步变化
                        BroadcastState<String, TableProcess> broadcastState = ctx.getBroadcastState(mapStateDescriptor);

                        String key = getKey(value.getSourceTable(), value.getSourceType());

                        if ("d".equals(value.getOp())) {
                            //删除配置
                            broadcastState.remove(key);
                            //保证初始的map和最新的维度配置是同步的
                            configMap.remove(key);
                        } else {
                            broadcastState.put(key, value);
                            configMap.put(key, value);
                        }

                    }
                });

    }


    private SingleOutputStreamOperator<TableProcess> getDwdConfig(StreamExecutionEnvironment env) {

        //读取变化记录，封装为json字符串
        MySqlSource<String> mySqlSource = MySqlSource
                .<String>builder()
                .hostname(ConfigUtil.getString("MYSQL_HOST"))
                .port(ConfigUtil.getInt("MYSQL_PORT"))
                .databaseList(ConfigUtil.getString("CONFIG_DATABASE")) // set captured database
                .tableList(ConfigUtil.getString("CONFIG_DATABASE")+"."+ConfigUtil.getString("CONFIG_TABLE")) // set captured table
                .username(ConfigUtil.getString("MYSQL_USER"))
                .password(ConfigUtil.getString("MYSQL_PASSWORD"))
                //默认就是initial。先读快照，再消费binlog
                .startupOptions(StartupOptions.initial())
                .deserializer(new JsonDebeziumDeserializationSchema()) // converts SourceRecord to JSON String
                .build();

        //维度表配置的变更要遵守顺序。这里全程从读取到处理到广播到下游都使用一个并行度
        return env
                .fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "config").setParallelism(1)
                .filter(str -> str.contains("DWD")).setParallelism(1)
                .map(new MapFunction<String, TableProcess>()
                {
                    @Override
                    public TableProcess map(String jsonStr) throws Exception {

                        JSONObject jsonObject = JSON.parseObject(jsonStr);
                        //配置表中没有op列，但是后续需要采集op列，判断配置信息是更新了还是删除了
                        String op = jsonObject.getString("op");
                        TableProcess t = null;
                        //取其他列
                        if ("d".equals(op)) {
                            //删除，取before
                            t = JSON.parseObject(jsonObject.getString("before"), TableProcess.class);
                        } else {
                            //CUR，取after
                            t = JSON.parseObject(jsonObject.getString("after"), TableProcess.class);
                        }

                        t.setOp(op);
                        return t;
                    }
                }).setParallelism(1);


    }

    private SingleOutputStreamOperator<String> etl(DataStreamSource<String> ds) {

        //定义type
        HashSet<String> opSets = Sets.newHashSet("insert", "update", "delete");

        return ds
                .filter(new FilterFunction<String>()
                {
                    @Override
                    public boolean filter(String jsonStr) throws Exception {

                        try {
                            JSONObject jsonObject = JSON.parseObject(jsonStr);
                            //正常的json结构
                            //抽取要验证的字段
                            String database = jsonObject.getString("database");
                            String table = jsonObject.getString("table");
                            String type = jsonObject.getString("type");
                            String data = jsonObject.getString("data");
                            String ts = jsonObject.getString("ts");

                            //StringUtils.isNoneBlank( 参数列表 )： 参数列表中任意一个字符串都不是NULL，白字符，''返回true
                            return ConfigUtil.getString("BUSI_DATABASE").equals(database)
                                    &&
                                    StringUtils.isNoneBlank(table, type, data, ts)
                                    &&
                                    opSets.contains(type)
                                    &&
                                    data.length() > 2;

                        } catch (Exception e) {
                            //不是json
                            log.warn(jsonStr + " 是非法的json格式.");
                            return false;
                        }
                    }
                })
                ;


    }

}
