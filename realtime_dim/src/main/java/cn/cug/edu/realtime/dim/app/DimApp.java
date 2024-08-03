package cn.cug.edu.realtime.dim.app;

import cn.cug.edu.common.base.BaseDataStreamApp;
import cn.cug.edu.common.util.ConfigUtil;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * author song
 * date 2024-08-03 21:37
 * Desc dimapp 用于继承baseapp 来实现job操作
 */
public class DimApp extends BaseDataStreamApp {

    //每一个job必须要有一个main方法来实现job的操作
    public static void main(String[] args) {
        new DimApp().start(10001,4,"dim_app_0803", ConfigUtil.getString("TOPIC_ODS_DB"));
    }


    @Override
    protected void hadle(StreamExecutionEnvironment env, DataStreamSource<String> kfSource) {

        //先简单的输出
        kfSource.print();


    }
}
