package cn.cug.edu.dws.app;


import cn.cug.edu.common.base.BaseDataStreamApp;
import cn.cug.edu.common.constant.GmallConstant;
import cn.cug.edu.common.function.DorisMapFunction;
import cn.cug.edu.common.util.DateFormatUtil;
import cn.cug.edu.common.util.DorisUtil;
import cn.cug.edu.dws.app.bean.HomeDetailPVBean;
import com.alibaba.fastjson.JSON;
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
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * author song
 * date 2025-01-14 14:09
 * Desc 这个指标直接用sql方式来实现
 */
public class TrafficHomeDetailPageView extends BaseDataStreamApp {

    public static void main(String[] args) {
        new TrafficHomeDetailPageView().start(
                12003,4, GmallConstant.DWS_TRAFFIC_HOME_DETAIL_PAGE_VIEW_WINDOW,
                GmallConstant.TOPIC_DWD_TRAFFIC_PAGE
        );
    }


    @Override
    protected void hadle(StreamExecutionEnvironment env, DataStreamSource<String> kfSource) {

        //1.把json格式的数据，封装为pojo，顺便进行标准化处理
        SingleOutputStreamOperator<HomeDetailPVBean> pojoDS = parseToPojo(kfSource);

        //pojoDS.print();
        //2.开窗计算
        SingleOutputStreamOperator<HomeDetailPVBean> aggDs = agg(pojoDS);

//        aggDs.print();
//        //3.输出
        writeToDoris(aggDs);

    }

    private void writeToDoris(SingleOutputStreamOperator<HomeDetailPVBean> aggDs) {
        aggDs
                .map(new DorisMapFunction<>())
                .sinkTo(DorisUtil.getDorisSink("cug_rt_gmall.dws_traffic_home_detail_page_view_window"));
    }

    private SingleOutputStreamOperator<HomeDetailPVBean> agg(SingleOutputStreamOperator<HomeDetailPVBean> pojoDS) {

        WatermarkStrategy<HomeDetailPVBean> watermarkStrategy = WatermarkStrategy
                .<HomeDetailPVBean>forMonotonousTimestamps()
                .withTimestampAssigner( (e, ts) -> e.getTs())
                //上游有多个并行度，其中有若干个并行度水印不推进，导致下游无法计算，可以设置存活时间。
                //上游的并行度长期不推进水印，就退出水印的发送队列
                .withIdleness(Duration.ofSeconds(10));
        return pojoDS
                .assignTimestampsAndWatermarks(watermarkStrategy)
                .windowAll(TumblingEventTimeWindows.of(Time.seconds(5)))
                //优先选择滚动聚合 reduce,aggregate,系统提供的sum,min,max...
                .reduce(new ReduceFunction<HomeDetailPVBean>() {
                            @Override
                            public HomeDetailPVBean reduce(HomeDetailPVBean value1, HomeDetailPVBean value2) throws Exception {
                                value1.setHome_uv_ct(value1.getHome_uv_ct() + value2.getHome_uv_ct());
                                value1.setGood_detail_uv_ct(value1.getGood_detail_uv_ct() + value2.getGood_detail_uv_ct());
                                return value1;
                            }
                        }, new ProcessAllWindowFunction<HomeDetailPVBean, HomeDetailPVBean, TimeWindow>() {
                            @Override
                            public void process(Context context, Iterable<HomeDetailPVBean> iterable, Collector<HomeDetailPVBean> collector) throws Exception {

                                TimeWindow window = context.window();

                                HomeDetailPVBean HomeDetailPVBean = iterable.iterator().next();
                                HomeDetailPVBean.setStt(DateFormatUtil.tsTodateTime(window.getStart()));
                                HomeDetailPVBean.setEdt(DateFormatUtil.tsTodateTime(window.getEnd()));
                                //[2024-01-06 23:59:55,2024-01-07 00:00:00)  使用starttime获取统计日期
                                HomeDetailPVBean.setCurDate(DateFormatUtil.tsToDate(window.getStart()));
                                collector.collect(HomeDetailPVBean);
                            }

                        }

                );

    }

    private SingleOutputStreamOperator<HomeDetailPVBean> parseToPojo(DataStreamSource<String> ds) {


        return ds
                .map(s -> JSON.parseObject(s, HomeDetailPVBean.class))
                .filter(b -> "home".equals(b.getPageId()) || "good_detail".equals(b.getPageId()))
                .keyBy(b -> b.getMid() + "_" + b.getPageId())
                .process(new KeyedProcessFunction<String, HomeDetailPVBean, HomeDetailPVBean>() {

                    //记录是最近访问日期，通过当前数据的eventtime和lastVisitDate比对，判断当前日期是否已经是一个访问过的设备
                    private ValueState<String> lastVisitDate;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        lastVisitDate = getRuntimeContext().getState(new ValueStateDescriptor<>("lastVisitDate", String.class));
                    }

                    @Override
                    public void processElement(HomeDetailPVBean value, Context ctx, Collector<HomeDetailPVBean> out) throws Exception {
                        String lastVisitDateStr = lastVisitDate.value();
                        long ts = value.getTs();
                        String eventDate = DateFormatUtil.tsToDate(ts);

                    /*
                        不相等:
                          lastVisitDate = null,这是当前设备的第一次访问。
                           lastVisitDate != null,跨天
                     */
                        if (!eventDate.equals(lastVisitDateStr)) {
                            if ("home".equals(value.getPageId())) {
                                value.setHome_uv_ct(1l);

                            } else {
                                value.setGood_detail_uv_ct(1l);
                            }
                            //标准化处理之后，发送数据到下游
                            out.collect(value);
                            lastVisitDate.update(eventDate);
                        }
                    }


                });
    }
}
