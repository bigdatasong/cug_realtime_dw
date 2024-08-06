package cn.cug.edu.realtime.dim.app;

import cn.cug.edu.common.base.BaseDataStreamApp;
import cn.cug.edu.common.util.ConfigUtil;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.fasterxml.jackson.databind.jsonFormatVisitors.JsonObjectFormatVisitor;
import com.google.common.collect.Sets;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.HashSet;
import java.util.Set;

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
}
