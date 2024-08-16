package cn.cug.edu.dwd.log.app;

import cn.cug.edu.common.base.BaseDataStreamApp;
import cn.cug.edu.common.constant.GmallConstant;
import cn.cug.edu.common.util.ConfigUtil;
import cn.cug.edu.common.util.DateFormatUtil;
import cn.cug.edu.common.util.KafkaUtil;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.fasterxml.jackson.databind.jsonFormatVisitors.JsonObjectFormatVisitor;
import com.ibm.icu.util.Output;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.summarize.aggregation.ValueSummaryAggregator;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.apache.hadoop.util.ConfTest;

import java.awt.color.CMMException;
import java.time.LocalDate;

/**
 * author song
 * date 2024-08-09 20:29
 * Desc 实现对流量域中的日志进行分流
 *      首先同样需要etl
 *      日志数据中有七个字段，common start page displays actions err ts
 *      其中只有dispalys actions是数组类型 其他就是json格式
 *      日志分成两类 启动日志和页面日志
 *      分流后：将日志分成五类
 *      启动日志 一定有start字段 （公共字段 common和ts）
 *      页面日志 一定有page字段 可能会有displays actions
 *      曝光日志  一定的有page字段，因为曝光日志就是页面日志的一种 一定会有displays字段
 *      动作日志 一定有page字段，因为动作日志一定是页面日志一种， 一定会有actions字段
 *      以上都可能会有err字段
 *      错误日志 一定有err字段 可能会有page字段也可能会有start字段，page字段和start字段不能同时存在，因为日志就只分为启动日志以及页面日志
 *      两种都有可能会有错误日志
 *
 */
@Slf4j
public class DwdBaseLog extends BaseDataStreamApp {

    public static void main(String[] args) {
        //在main方法中执行
        new DwdBaseLog().start(
                11001,
                4,
                "dwd_traffic_baselog",
                ConfigUtil.getString("TOPIC_ODS_LOG")

        );
    }

    @Override
    protected void hadle(StreamExecutionEnvironment env, DataStreamSource<String> kfSource) {

        //先进行etl
        //思路就是先过滤非json字段数据 然后只包含这五种日志格式，并且里面字段必须要有数据
        etl(kfSource);

        //接下来就是新老客户的的一个纠正，
        /**
         * 日志数据 common 字段下的 is_new 字段是用来标记新老访客状态的，1 表示新访客，0表示老访客。
         * 前端埋点采集到的数据可靠性无法保证，可能会出现老访客被标记为新访客的问题，因此需要对该标记进行修复
         * 为啥保证保证不了呢，埋点数据会缓存一个标记，如果首日当天 is_new 就会标记为1，并且时限是一天，第二天它就会变成0，
         * 但是如果说用户清楚了缓存或者说卸载了之类的状况，这个缓存就会没有，这样的话同一个设备又是is_new =1 ，
         * 所以我们需要保证的是同一个设备的用户只能在首日当天isnew是1 其余状况就只能是0
         */
        //思路：利用状态，为每个mid维护一个单值状态，这个状态存放的是需要记录当前设备的首次访问日期，然后对每一条数据中ts和这个首次访问日期来进行比较
        //并且还要根据埋点中的is_new的值来联合判断
        //因为要和ts来比较 所以在这定义一些日期工具类 来实现ts毫秒值和日期字符串的转换。方便比较

        //工具类完成以后 定义一个方法实现对新老客户的矫正
        SingleOutputStreamOperator<JSONObject> correctNewUser = correctNewUser(kfSource);

        //可以调试的时候用
        correctNewUser.print();

        //接下来就是写出到kafka中，需要一个kafkasink工具类 返回一个kafkasink

        //写好工具类以后 可以定义一个方法将数据分流写出
        divideStreamWriteToKafka(correctNewUser);


    }




    private void etl(DataStreamSource<String> kfSource) {
        //执行过滤操作
        kfSource.filter(new FilterFunction<String>() {
            @Override
            public boolean filter(String s) throws Exception {
                try {
                    JSONObject jsonObject = JSON.parseObject(s);
                    //判断他的key时候是指定的五种日志数据类型
                    return (jsonObject.containsKey(GmallConstant.START) && jsonObject.getString(GmallConstant.START).length() > 2)
                            || (jsonObject.containsKey(GmallConstant.PAGE) && jsonObject.getString(GmallConstant.PAGE).length() > 2)
                            || (jsonObject.containsKey(GmallConstant.DISPLAY) && jsonObject.getJSONArray(GmallConstant.DISPLAY).size() > 0)
                            || (jsonObject.containsKey(GmallConstant.ACTION) && jsonObject.getJSONArray(GmallConstant.ACTION).size() > 0)
                            || (jsonObject.containsKey(GmallConstant.ERR) && jsonObject.getString(GmallConstant.ERR).length() >2);
                } catch (Exception e) {
                    //如果解析不成功说明不是json数据
                    //给出日志提示 并且返回
                    log.warn("当前日志格式不是json格式" + s);
                    return false;
                }

            }
        });

    }


    // 实现对新老客户的矫正
    private SingleOutputStreamOperator<JSONObject> correctNewUser(DataStreamSource<String> kfSource) {
        //首先需要对mid进行keyby 因为要保证的是对每个设备id都维护一个状态
        //mid在common字段中，而上游传递过来的是一个string ，所以可以先解析成jsonobject 然后通过方法来取mid
        SingleOutputStreamOperator<JSONObject> process = kfSource.keyBy((t -> JSON.parseObject(t).getJSONObject("common").getString("mid")))
                .process(new KeyedProcessFunction<String, String, JSONObject>() {
                    //最终转为jsonobject 为什么使用process 方法是因为这里有open方法可以初始化状态
                    //本质不会改变什么只是将is_new按照真实情况进行修改
                    //先在open方法中定义好状态

                    // 定义状态
                    private ValueState<String> firstVisitDate;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        //初始化好状态 状态存放的是mid首次访问日期
                        firstVisitDate = getRuntimeContext().getState(new ValueStateDescriptor<>("firstVisitDate", String.class));
                    }

                    @Override
                    public void processElement(String s, Context context, Collector<JSONObject> collector) throws Exception {
                        //将原始数据解析成jsonobject
                        JSONObject originalData = JSON.parseObject(s);
                        //先将common取出
                        JSONObject common = originalData.getJSONObject("common");
                        //将is_new取出
                        String isNew = common.getString("is_new");
                        //将ts取出
                        Long ts = originalData.getLong("ts");
                        //将ts转成日期string就行
                        String toDate = DateFormatUtil.tsToDate(ts);
                        //获取当前的mid 方便后面的日志打印
                        String mid = context.getCurrentKey();

                        //取出状态的首次访问时间
                        String value = firstVisitDate.value(); //注意要取出的是value的值 而不是判断状态是否为空
                        //状态是一定不为空的，而是要取出里面的value值
                        /**
                         * 接下来就是判断首次访问日期和当前ts得比较
                         *    如果说当前的状态中的首次访问时间为null 还需要根据isnew来判断情况，如果说isnew是1说明是新用户，
                         *    那么这个情况就是用户首次访问，那么此时就需要给value赋值上此时的ts
                         *    如果说isnew是0 说明什么说明他是老用户，那么为什么说老用户他的状态还是null，会存在埋点的数据是在之前就有的，
                         *    而流式状态很有可能是后面做出来的，即相当于是历史数据一样，那么此时还是要给状态赋值，只不过这次赋值就赋值成当前
                         *    ts之前的日期即可
                         *
                         *    如果说当前的状态中的首次访问时间不为null 值也有两种情况，是当日或者不是当日，并且还要根据isnew再来排列组合
                         *    如果说是当日 并且isnew 是为1 说明就是当日的情况并且也是新用户 而且状态的值也是当日 所以说不做任何操作
                         *    如果说是当日 并且isnew 是0 这个情况是不存在的，因为如果是isnew为0的话说明是老用户 那么他的状态肯定不是当日，
                         *
                         *    如果说不是当日，并且isnew是1 说明他是新用户，此时就需要重置isnew为0 因为这就是说明我们要修改的情况
                         *    如果说isnew为0 说明是老用户 这个就不需要操作，因为状态中不是当日的话，并且isnew也为0 说明就是正常情况。
                         *
                         */
                        if (value == null) {
                            if (GmallConstant.ISNEWEW.equals(isNew)) {
                                //说明状态中没有值， 并且他是新客户 所以需要将当前的ts表示的日期存入状态中
                                firstVisitDate.update(toDate);
                                //可以日志记录一下
                                log.warn("当前设备id" + mid + "是新客户" + "于" + toDate + "首次访问");
                            } else {
                                //说明他是老客户 但是他的状态中没有值，所以需要将状态的值更新到当前ts的前一天即可
                                //获取前一天的日期
                                String lastDate = LocalDate.parse(toDate).plusDays(-1).toString();
                                firstVisitDate.update(lastDate);
                                log.warn("当前设备" + mid + "是老客户" + "最晚在" + lastDate + "前访问过");
                            }

                        } else {
                            //当前状态不为空 但是也有可能是当天或者不是当天
                            //只有一种情况需要修复 就是当前状态中的值不是当天 但是isnew是1 这是需要修复的，因为按理来说当天的状态值不是当天的话
                            //就一定不是新用户需要修复
                            //而其他的情况就比如说 如果说状态不是当天 但是isnew是0 说明他是老客户 不用操作，
                            //如果说状态是当天，那么isnew只有一种情况就是他是1 同样不用操作，因为他肯定是新客户 并且状态时当天
                            //不可能说他的isnew还是0 即他是老客户 但是状态又是当天的。
                            //关键在于比较当前状态不是当天
                            if (GmallConstant.ISNEWEW.equals(isNew) && !LocalDate.now().toString().equals(toDate)) {
                                //修复isnew 就需要将common中的isnew 改为0
                                common.put("is_new", "0");
                                //日志记录一下
                                log.warn("当前用户" + mid + "是老客户，需要进行修复,于" + toDate + "首次访问过");
                            }

                        }

                        collector.collect(originalData);


                    }
                });

        return process;
    }

    //实现将日志数据进行分流写出
    //分流写出的前提就是 要知道每一种日志需要写出的东西是什么
    /**
     *  首先 common以及ts肯定都要有的 作为公共的字段
     *  其次 start启动日志必须有start，page页面日志必须要有page字段。
     *  display曝光字段，因为他是数组形式 所以他需要将里面每一条都要写出 需要炸裂操作
     *  action 也是需要 另外需要将每一个action中ts替换成真正的每一个action的ts
     *  err：就是可能在start日志或者说page中可能会有
     *
     *  为了方便起见，我们需要将数据进行扁平化操作，现在上游传递过来的jsonobject对象是一个嵌套的json对象，即json对象中又有json对象
     *  为了后续方便映射 我们可以写出一个jsonstring 这个jsonstring中没有嵌套格式 就是一个json
     *
     *
     *
     * @param correctNewUser
     */
    private void divideStreamWriteToKafka(SingleOutputStreamOperator<JSONObject> correctNewUser) {

        //定义测流输出标记
        OutputTag<String> page = new OutputTag<>(GmallConstant.PAGE, Types.STRING);
        OutputTag<String> err = new OutputTag<>(GmallConstant.ERR, Types.STRING);
        OutputTag<String> action = new OutputTag<>(GmallConstant.ACTION, Types.STRING);
        OutputTag<String> display = new OutputTag<>(GmallConstant.DISPLAY, Types.STRING);


        SingleOutputStreamOperator<String> startDs = correctNewUser.process(new ProcessFunction<JSONObject, String>() {


            //在算子中自己定义方法实现上述需求
            //先定义一个只返回一个jsonobject的方法 因为我们最后就是只写出一个jsongstrin字符串中只有一个json 没有嵌套的json
            // 因为 common page start都是json格式所以不妨将这些数据都转为一个json即可 即返回一个jsonobject对象
            //因为common和ts是公共的，即无论是页面日志还是启动日志都需要的，所以说参数可以是他们两个
            //另外原始数据也需要传递进去，还有就是定义type 用于表示他是页面日志还是启动日志，
            //另外这个方法将来就是在processelement中调用。
            //总之这个方法就是将合成一个jsonobject返回只有一个json格式，将common、ts 以及（page或者start来合并进去）
            private JSONObject parseData(JSONObject common, Long ts, String type, JSONObject originalData) {
                //先将common和ts合并
                common.put("ts", ts);
                //将type在原始数据中的类型一次性封装到一个jsonobject中
                JSONObject typeObject = originalData.getJSONObject(type);
                common.putAll(typeObject);

                return common;

            }

            //还需要定义一个方法实现数组类型的扁平化，上一个方法就是把所有非数组类型的json格式转为一个jsonobject输出，现在就是在它的基础上
            //需要把数组形式的格式进行扁平化，然后实现一个jsonobject中只有一个jsonstring输出
            //对于方法的定义中 我可以不需要方法的返回值，就直接往下游输出 因为我这个方法最终是要在processelement中使用的，也是在前一个方法中的基础
            //上使用的，并且这些都是在测流输出的，所以可以传一个context参数给他方便它进行测流输出 另外方便起见把测流输出标记也传递给他
            //方法需要的参数有首先就是包含了上述方法的返回值，即以及包含了common、ts、以及page字段、需要明确的是在此次日志数据中，数组类型的字段
            //中都是页面日志才有可能有的，也即start日志是肯定没有的，所以说他是包含了page字段的，同样也需要一个type类型，用于表示他是曝光还是动作
            //因为在action数组中的每一条数据都有一个ts。而这个ts和前面已经存在的ts需要取一个，肯定是需要将前者的ts替换到后者的ts中，这样才能表示的是
            //每一条action数据的ts
            private void handleActionsOrDisplays(JSONObject page, String type, JSONObject oridinalDate, Context context, OutputTag<String> tag) {

                //获取type类型的数据
                JSONArray dateJSONArray = oridinalDate.getJSONArray(type);
                //因为是数组形式需要对其进行遍历出每一条数据来加入到page中
                for (int i = 0; i < dateJSONArray.size(); i++) {

                    //获取里面每一个的jsonobject
                    JSONObject jsonObject = dateJSONArray.getJSONObject(i);
                    //将page字段中的全部加入
                    jsonObject.putAll(page);
                    //还有就是对于action的类型需要替换page中的ts
                    if (GmallConstant.ACTION.equals(type)) {
                        jsonObject.put("ts", jsonObject.getString("ts"));
                    }

                    // 都需要进行测流输出
                    context.output(tag, jsonObject.toString());

                }


            }

            public void processElement(JSONObject jsonObject, Context context, Collector<String> collector) throws Exception {

                //获取common字段，获取ts，然后依次判断是什么类型的日志数据，然后定义主流和测流，start定为主流 其他都从四个测流输出
                JSONObject common = jsonObject.getJSONObject("common");
                //因为ts需要long类型的数据 所以我们可以通过getlong来获取
                Long ts = jsonObject.getLong("ts");
                //依次判断各种类型的日志数据 然后分流输出
                if (jsonObject.containsKey(GmallConstant.START)) {
                    //说明是启动日志调用方法进行 封装 并且主流输出
                    JSONObject startData = parseData(common, ts, GmallConstant.START, jsonObject);
                    //主流输出
                    collector.collect(startData.toString());
                } else {
                    //说明一定是page日志，但是也有可能还是动作或者曝光日志，无论是什么都需要测流输出，测流输出需要测流输出标记，所以需要在前面先定义测流输出标记

                    //定义好测流输出标记以后 如果是page日志 先调用解析方法 然后输出
                    JSONObject pageData = parseData(common, ts, GmallConstant.PAGE, jsonObject);
                    //测流输出
                    context.output(page, pageData.toString());

                    //还有数组情况
                    if (jsonObject.containsKey(GmallConstant.ACTION)) {
                        //说明是启动日志 先调用第一个解析方法 然后调用数组专用解析 方法 因为在前面的数组操作方法中已经将context以及outputtag类型传递进去了
                        //所以直接调用即可
                        handleActionsOrDisplays(pageData, GmallConstant.ACTION, jsonObject, context, action);
                    }

                    // 如果是dispalys
                    if (jsonObject.containsKey(GmallConstant.DISPLAY)) {
                        handleActionsOrDisplays(pageData, GmallConstant.DISPLAY, jsonObject, context, display);
                    }
                }
                //最后一种情况就是 不管是启动还是页面都可能会有err日志

                if (jsonObject.containsKey(GmallConstant.ERR)) {
                    //同样进行解析抽取 然后测流输出
                    JSONObject errData = parseData(common, ts, GmallConstant.ERR, jsonObject);
                    context.output(err, errData.toString());
                }


            }
        });

        //写入到kafka中
        startDs.sinkTo(KafkaUtil.getKfSink(GmallConstant.TOPIC_DWD_TRAFFIC_START));
        startDs.getSideOutput(page).sinkTo(KafkaUtil.getKfSink(GmallConstant.TOPIC_DWD_TRAFFIC_PAGE));
        startDs.getSideOutput(err).sinkTo(KafkaUtil.getKfSink(GmallConstant.TOPIC_DWD_TRAFFIC_ERR));
        startDs.getSideOutput(action).sinkTo(KafkaUtil.getKfSink(GmallConstant.TOPIC_DWD_TRAFFIC_ACTION));
        startDs.getSideOutput(display).sinkTo(KafkaUtil.getKfSink(GmallConstant.TOPIC_DWD_TRAFFIC_DISPLAY));

    }
}
