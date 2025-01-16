package cn.cug.edu.common.base;

import cn.cug.edu.common.constant.GmallConstant;
import cn.cug.edu.common.util.ConfigUtil;
import cn.cug.edu.common.util.SqlUtil;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.Duration;


/**
 * author song
 * date 2024-08-16 18:48
 * Desc  作为flinksql编程的base app
 */
public abstract class BaseStreamTableApp {

    // 定义start方法
    public void start(int port,int parallelism,String jobName){
        //首先就是先创建流环境， 也就是说对于table环境使用流环境来创建
        // 每个job 都应该有web端口方便调试 端口号应该是子类实现该方法来传递
        Configuration configuration = new Configuration();
        configuration.setInteger("rest.port",port);
        //相当于代码中 WaterMarkStradegy设置 idleness()
        configuration.setString("table.exec.source.idle-timeout","10s");
        //创建流环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(configuration);
        //2.设置并行度 每个job应该是不一样的
        env.setParallelism(parallelism);
        //设置状态后端 以及开启checkpoint 其中状态后端就使用默认的，
        //3.开启checkpoint
        env.enableCheckpointing(ConfigUtil.getInt("CK_INTERVAL"));
        // 设置ck的存储目录
        CheckpointConfig checkpointConfig = env.getCheckpointConfig();
        checkpointConfig.setCheckpointStorage(ConfigUtil.getString("CK_STORAGE") + jobName);

        //设置ck的超时时间
        checkpointConfig.setCheckpointTimeout(5 * 6000);
        //两次ck之间，必须间隔500ms
        checkpointConfig.setMinPauseBetweenCheckpoints(2000);
        //设置ck的最大的并发数. 非对齐，强制为1
        checkpointConfig.setMaxConcurrentCheckpoints(1);
        //如果开启的是Barrier对齐，那么当60s还没有对齐完成，自动转换为非对齐
        checkpointConfig.setAlignedCheckpointTimeout(Duration.ofSeconds(60));
        //默认情况，Job被cancel了，存储在外部设备的ck数据会自动删除。你可以设置永久持久化。
        checkpointConfig.setExternalizedCheckpointCleanup(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        //一旦ck失败到达10次，此时job就终止
        checkpointConfig.setTolerableCheckpointFailureNumber(10);
        // 10. job 失败的时候重启策略
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3,3000));

        //创建表环境 并且将流环境和表环境 都传给子类要实现的方法，在这里为啥不需要说利用sql来创建一个kafkasource呢，是因为
        //可以定义一个公共方法来创建kafkasource 即利用sql的方式 ，通过子类来实现这个公共方法也是一样的，只是换一种方式而已
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        handle(tableEnv,env);


    }

    protected abstract void handle(StreamTableEnvironment tableEnv, StreamExecutionEnvironment env);


    //定义创建ods_db表的方法 使用flinksql来映射
    protected void createOdsDB(StreamTableEnvironment tableEnv,String groupId){
        //在flinksql中需要通过连接器来映射 创建表地时候需要tableenv来执行sql 所以环境需要参数传递进来
        //定义连接器sql
        //分析字段可知，data和old中的字段里面的属性不是固定的，所以只能用map 因为这是 flink和json解析时需要导入依赖的
        //而且列名必须和json中的一样，但是顺序可以不一样，
        String sql = "create table ods_db (" +
                " `database` string ," +
                " `table` string ," +
                " `type` string ," +
                " `ts` bigint ," +
                " `data` map<string,string> ," +
                " `old` map<string,string> ," +
                "  `pt` as PROCTIME() ," +
                "   et as TO_TIMESTAMP_LTZ(ts,0) ," +
                "   WATERMARK FOR et as et - INTERVAL '0.001' SECOND " +
                  SqlUtil.getKafkaSourceConfigSql(ConfigUtil.getString("TOPIC_ODS_DB"),groupId);

        //使用tableenv来执行sql
        tableEnv.executeSql(sql);
    }

    //定义创建habse中维度表的映射 通过flinksql来实现
    //首先对于habse连接器的要求参数中，创建表的映射时 首先需要定义rowkey 作为列，列名可以随意 但是类型必须为string类型
    // 然后就是要以列族的单位去申明列 一般来说列族名就是列名，类型必须是row ，然后在row中定义每个列族中的列的类型和列名，
    //最后就是必须设置主键约束
    //因为需要映射的是hbase中的base_dic表 通过配置表去查它的需要的字段可知 他只有一个列族，列族中除了主键以外只有一个列 即dic_name
    protected void createDicCode(StreamTableEnvironment tableEnv){
        String sql = "create table dim_dic_code(" +
                " id STRING," +
                " info Row<dic_name STRING>," +
                " PRIMARY KEY (id) NOT ENFORCED" +
                ")" + SqlUtil.getHbaseSourceSql("cug_rt_gmall:dim_base_dic");

        tableEnv.executeSql(sql);
    }
}
