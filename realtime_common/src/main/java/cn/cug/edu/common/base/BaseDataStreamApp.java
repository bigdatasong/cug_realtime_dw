package cn.cug.edu.common.base;

import cn.cug.edu.common.util.ConfigUtil;
import cn.cug.edu.common.util.KafkaUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * author song
 * date 2024-08-03 19:59
 * Desc 用于datastream中的模板app
 */
public abstract class BaseDataStreamApp {

    //start方法定义了flink job的一些固定流程的操作，当子类继承该类时就可以调用该方法
    public void start(int port,int p,String ckAndGroupIdAndJobName,String topic){

        //获取环境 方便调试 每个job都有自己的web端口,所以这个端口号不应该固定，而是子类调用该方法时传递进来，所以需要在start中作为参数来传递
        Configuration configuration = new Configuration();
        configuration.setInteger("rest.port",port);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(configuration);
        //一些通用的参数 需要提前设置好
        //1. 并行度的设置 每个job应该都不一样 所以需要参数传递
        env.setParallelism(p);
        //checkpoint的设置
        // 状态后端使用默认的，
        //2.开启checkpoint 每多久备份一次的参数可以在配置文件中定义好
        env.enableCheckpointing(ConfigUtil.getInt("CK_INTERVAL"));
        //设置ck的存储目录 存储目录的路径应该有一样的前缀 以及可以用每个job名字为后缀拼接
        //而每个job名字也是不一样的，需要参数传递
        CheckpointConfig checkpointConfig = env.getCheckpointConfig();
        checkpointConfig.setCheckpointStorage(ConfigUtil.getString("CK_STORAGE") + ckAndGroupIdAndJobName);
        // 设置checkpoint的并发数
        checkpointConfig.setMaxConcurrentCheckpoints(1); //默认也是1
        // 6. 设置两个 checkpoint 之间的最小间隔. 如果这设置了, 则可以忽略setMaxConcurrentCheckpoints
        checkpointConfig.setMinPauseBetweenCheckpoints(500);
        // 7. 设置 checkpoint 的超时时间 5min
        checkpointConfig.setCheckpointTimeout(300000);
        // 8. 当 job 被取消的时候, 存储从 checkpoint 的数据是否要删除
        checkpointConfig.setExternalizedCheckpointCleanup(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        // 9. 开启非对齐检查点
        // env.getCheckpointConfig().enableUnalignedCheckpoints();
        // env.getCheckpointConfig().setForceUnalignedCheckpoints(true);
        // 10. job 失败的时候重启策略
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3,3000));

        //接下来就是封装一个方法 这个方法必须是子类实现，子类方法中逻辑就是从kafka中读取数据 然后进行流式转换的操作，因为都是从kafka中读取数据
        //所以可以封装一个kafka工具类方法实现返回一个kafkasource，
        //因为子类需要实现流式转换操作，需要env才能进行 所以参数就是env 以及kafkasource

        //编写hadle方法 用于子类实现的方法 这个hadle方法在start方法中调用
        //参数的话 start方法也是不知道的 所以需要外界传入
        //因为source是一样的所以可以先获取后再传入
        KafkaSource<String> kfSource = KafkaUtil.getKfSource(ckAndGroupIdAndJobName, topic);
        DataStreamSource<String> kfsource = env.fromSource(kfSource, WatermarkStrategy.noWatermarks(), "kfsource");

        hadle(env, kfsource);

        //处理完成以后 env提交也是相同的
        try {
            env.execute(ckAndGroupIdAndJobName);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

    }

    //这个方法必须是子类实现 不能通过new 父类来调用该方法 所以权限设置成protected 还有就是
    //必须子类实现 所以要设置成抽象的方法 又因为抽象方法必须在抽象类上，所以这个baseapp也是抽象的
    protected abstract void hadle(StreamExecutionEnvironment env, DataStreamSource<String> kfSource);
}
