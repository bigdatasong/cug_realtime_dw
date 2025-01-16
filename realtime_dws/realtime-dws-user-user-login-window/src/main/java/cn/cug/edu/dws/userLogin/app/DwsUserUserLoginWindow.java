package cn.cug.edu.dws.userLogin.app;

import cn.cug.edu.common.base.BaseDataStreamApp;
import cn.cug.edu.common.constant.GmallConstant;
import cn.cug.edu.common.function.DorisMapFunction;
import cn.cug.edu.common.util.DateFormatUtil;
import cn.cug.edu.common.util.DorisUtil;
import cn.cug.edu.dws.userLogin.app.bean.UserLoginBean;
import com.alibaba.fastjson.JSON;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

/**
 * author song
 * date 2025-01-14 15:36
 * Desc
 */
public class DwsUserUserLoginWindow extends BaseDataStreamApp {

    public static void main(String[] args) {
        new DwsUserUserLoginWindow().start(
                13004,4, GmallConstant.DWS_USER_USER_LOGIN_WINDOW,
                GmallConstant.TOPIC_DWD_TRAFFIC_PAGE
        );
    }

    @Override
    protected void hadle(StreamExecutionEnvironment env, DataStreamSource<String> kfSource) {

        //1.把json格式的数据，封装为pojo，顺便进行标准化处理
        SingleOutputStreamOperator<UserLoginBean> pojoDS = parseToPojo(kfSource);

//        pojoDS.print();
//        2.开窗计算
        SingleOutputStreamOperator<UserLoginBean> aggDs = agg(pojoDS);

//        aggDs.print();
//        //3.输出
        writeToDoris(aggDs);

    }

    private void writeToDoris(SingleOutputStreamOperator<UserLoginBean> aggDs) {
        aggDs
                .map(new DorisMapFunction<>())
                .sinkTo(DorisUtil.getDorisSink("cug_rt_gmall.dws_user_user_login_window"));
    }

    private SingleOutputStreamOperator<UserLoginBean> agg(SingleOutputStreamOperator<UserLoginBean> pojoDS) {

        WatermarkStrategy<UserLoginBean> watermarkStrategy = WatermarkStrategy
                .<UserLoginBean>forMonotonousTimestamps()
                .withTimestampAssigner( (e, ts) -> e.getTs())
                //上游有多个并行度，其中有若干个并行度水印不推进，导致下游无法计算，可以设置存活时间。
                //上游的并行度长期不推进水印，就退出水印的发送队列
                .withIdleness(Duration.ofSeconds(10));
        return pojoDS
                .assignTimestampsAndWatermarks(watermarkStrategy)
                .windowAll(TumblingEventTimeWindows.of(Time.seconds(5)))
                //优先选择滚动聚合 reduce,aggregate,系统提供的sum,min,max...
                .reduce(new ReduceFunction<UserLoginBean>() {
                            @Override
                            public UserLoginBean reduce(UserLoginBean value1, UserLoginBean value2) throws Exception {
                                value1.setBackCt(value1.getBackCt() + value2.getBackCt());
                                value1.setUuCt(value1.getUuCt() + value2.getUuCt());
                                return value1;
                            }
                        }, new ProcessAllWindowFunction<UserLoginBean, UserLoginBean, TimeWindow>() {
                            @Override
                            public void process(Context context, Iterable<UserLoginBean> iterable, Collector<UserLoginBean> collector) throws Exception {

                                TimeWindow window = context.window();

                                UserLoginBean UserLoginBean = iterable.iterator().next();
                                UserLoginBean.setStt(DateFormatUtil.tsTodateTime(window.getStart()));
                                UserLoginBean.setEdt(DateFormatUtil.tsTodateTime(window.getEnd()));
                                //[2024-01-06 23:59:55,2024-01-07 00:00:00)  使用starttime获取统计日期
                                UserLoginBean.setCurDate(DateFormatUtil.tsToDate(window.getStart()));
                                collector.collect(UserLoginBean);
                            }

                        }

                );

    }

    private SingleOutputStreamOperator<UserLoginBean> parseToPojo(DataStreamSource<String> ds) {


        return ds
                .map(s -> JSON.parseObject(s, UserLoginBean.class))
                .filter(u -> StringUtils.isNotBlank(u.getUid()) && ("login".equals(u.getLast_page_id()) || u.getLast_page_id() == null))
                .keyBy(UserLoginBean::getUid)
                .process(new KeyedProcessFunction<String, UserLoginBean, UserLoginBean>()
                {
                    //维护一个最近访问日期，用来判断当前用户是否是回流的
                    private ValueState<String> lastVisitDate;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        lastVisitDate = getRuntimeContext().getState(new ValueStateDescriptor<>("lastVisitDate", String.class));
                    }

                    @Override
                    public void processElement(UserLoginBean value, Context ctx, Collector<UserLoginBean> out) throws Exception {
                        String lastVisitDateStr = lastVisitDate.value();
                        long ts = value.getTs();
                        String eventDate = DateFormatUtil.tsToDate(ts);

                    /*
                        不相等:
                          lastVisitDate = null,这是当前用户当天第一次访问。
                           lastVisitDate != null,跨天，这是当前用户当天第一次访问。
                     */
                        if (!eventDate.equals(lastVisitDateStr)) {
                            value.setUuCt(1l);
                            //更新lastVisitDate
                            lastVisitDate.update(eventDate);

                            if (lastVisitDateStr != null) {
                                //之前来过
                                long diffDays = TimeUnit.DAYS.convert(ts - DateFormatUtil.dateToTs(lastVisitDateStr), TimeUnit.MILLISECONDS);
                                if (diffDays > 7) {
                                    //最近7天没来，现在来了，符合回流条件
                                    value.setBackCt(1l);
                                }
                            }
                        }

                        //发送到下游
                        if (value.getUuCt() > 0) {
                            out.collect(value);
                        }
                    }
                });
    }
}
