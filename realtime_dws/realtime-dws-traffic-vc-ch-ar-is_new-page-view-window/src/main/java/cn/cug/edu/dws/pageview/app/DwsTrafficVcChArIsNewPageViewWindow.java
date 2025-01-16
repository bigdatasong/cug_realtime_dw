package cn.cug.edu.dws.pageview.app;

import cn.cug.edu.common.base.BaseDataStreamApp;
import cn.cug.edu.common.constant.GmallConstant;
import cn.cug.edu.common.function.DorisMapFunction;
import cn.cug.edu.common.util.DateFormatUtil;
import cn.cug.edu.common.util.DorisUtil;
import cn.cug.edu.dws.pageview.app.pojo.TrafficPageView;
import com.alibaba.fastjson.JSON;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * author song
 * date 2025-01-13 16:15
 * Desc 因为指标比较复杂 所以使用datastreamapi来实现
 */
@Slf4j
public class DwsTrafficVcChArIsNewPageViewWindow extends BaseDataStreamApp {

    public static void main(String[] args) {
        new DwsTrafficVcChArIsNewPageViewWindow().start(
                12001,4, GmallConstant.DWS_TRAFFIC_VC_CH_AR_IS_NEW_PAGE_VIEW_WINDOW,
                GmallConstant.TOPIC_DWD_TRAFFIC_PAGE

        );
    }

    @Override
    protected void hadle(StreamExecutionEnvironment env, DataStreamSource<String> kfSource) {

        //1.把json格式的数据，封装为pojo，顺便进行标准化处理
        SingleOutputStreamOperator<TrafficPageView> pojoDS = parseToPojo(kfSource);

        //pojoDS.print();
        //2.开窗计算
        SingleOutputStreamOperator<TrafficPageView> aggDs = agg(pojoDS);

//        aggDs.print();
//        //3.输出
        writeToDoris(aggDs);

    }


    private void writeToDoris(SingleOutputStreamOperator<TrafficPageView> aggDs) {
        aggDs
                .map(new DorisMapFunction<>())
                .sinkTo(DorisUtil.getDorisSink("cug_rt_gmall.dws_traffic_vc_ch_ar_is_new_page_view_window"));
    }

    private SingleOutputStreamOperator<TrafficPageView> agg(SingleOutputStreamOperator<TrafficPageView> pojoDS) {

        WatermarkStrategy<TrafficPageView> watermarkStrategy = WatermarkStrategy
                .<TrafficPageView>forMonotonousTimestamps()
                .withTimestampAssigner( (e, ts) -> e.getTs())
                //上游有多个并行度，其中有若干个并行度水印不推进，导致下游无法计算，可以设置存活时间。
                //上游的并行度长期不推进水印，就退出水印的发送队列
                .withIdleness(Duration.ofSeconds(10));
        return pojoDS
                .assignTimestampsAndWatermarks(watermarkStrategy)
                .keyBy(t -> t.getVc() + "_" + t.getCh() + "_" + t.getAr() + "_" + t.getIsNew())
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                //优先选择滚动聚合 reduce,aggregate,系统提供的sum,min,max...
                .reduce(new ReduceFunction<TrafficPageView>()
                        {
                            @Override
                            public TrafficPageView reduce(TrafficPageView value1, TrafficPageView value2) throws Exception {
                                value2.setSvCt(value1.getSvCt() + value2.getSvCt());
                                value2.setPvCt(value1.getPvCt() + value2.getPvCt());
                                value2.setDuringTime(value1.getDuringTime() + value2.getDuringTime());
                                value2.setUvCt(value1.getUvCt() + value2.getUvCt());
                                return value2;
                            }
                        }, new ProcessWindowFunction<TrafficPageView, TrafficPageView, String, TimeWindow>()
                        {
                            @Override
                            public void process(String key, Context context, Iterable<TrafficPageView> elements, Collector<TrafficPageView> out) throws Exception {

                                TimeWindow window = context.window();
                                log.warn("怎么还没到这");

                                TrafficPageView trafficPageView = elements.iterator().next();
                                trafficPageView.setStt(DateFormatUtil.tsTodateTime(window.getStart()));
                                trafficPageView.setEdt(DateFormatUtil.tsTodateTime(window.getEnd()));
                                //[2024-01-06 23:59:55,2024-01-07 00:00:00)  使用starttime获取统计日期
                                trafficPageView.setCurDate(DateFormatUtil.tsToDate(window.getStart()));
                                trafficPageView.setDurSum(trafficPageView.getDuringTime());
                                out.collect(trafficPageView);

                            }
                        }
                );

    }

    private SingleOutputStreamOperator<TrafficPageView> parseToPojo(DataStreamSource<String> ds) {

         /*
                对POJO进行标准化处理(按照后续聚合的需求，为计算的字段赋值)
                 `uv_ct`    BIGINT REPLACE COMMENT '独立访客数',
                      应该把同一个mid的数据分组，在当前统计日期中，如果mid是第一次出现，算uv_ct=1，否则为0
                         accessDate=2024-01-06,mid=1,pageView,uv_ct=1
                         accessDate=2024-01-06,mid=1,pageView,uv_ct=0
                         accessDate=2024-01-06,mid=1,pageView,uv_ct=0
                         accessDate=2024-01-07,mid=1,pageView,uv_ct=1
                         accessDate=2024-01-07,mid=1,pageView,uv_ct=0



                  `sv_ct`    BIGINT REPLACE COMMENT '会话数',
                          如果当前这条数据是这个会话的第一次访问，此时sv_ct=1，否则应为0
                         sid=1,ts=1,pageView,sv_ct=1
                         sid=1,ts=2,pageView,sv_ct=0
                         sid=1,ts=3,pageView,sv_ct=0
                         sid=1,ts=4,pageView,sv_ct=0


                    `pv_ct`     '页面浏览数', count  =  sum(1)
                 `dur_sum`   '累计访问时长'   sum
             */
        return ds
                .map(s -> JSON.parseObject(s, TrafficPageView.class))
                .keyBy(TrafficPageView::getMid)
                .process(new KeyedProcessFunction<String, TrafficPageView, TrafficPageView>()
                {

                    //记录是最近访问日期，通过当前数据的eventtime和lastVisitDate比对，判断当前日期是否已经是一个访问过的设备
                    private ValueState<String> lastVisitDate;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        lastVisitDate = getRuntimeContext().getState(new ValueStateDescriptor<>("lastVisitDate", String.class));
                    }

                    @Override
                    public void processElement(TrafficPageView value, Context ctx, Collector<TrafficPageView> out) throws Exception {
                        String lastVisitDateStr = lastVisitDate.value();
                        long ts = value.getTs();
                        String eventDate = DateFormatUtil.tsToDate(ts);

                    /*
                        不相等:
                          lastVisitDate = null,这是当前设备的第一次访问。
                           lastVisitDate != null,跨天
                     */
                        if (!eventDate.equals(lastVisitDateStr)) {
                            value.setUvCt(1l);
                            //更新lastVisitDate
                            lastVisitDate.update(eventDate);
                        }
                        //标准化处理之后，发送数据到下游
                        out.collect(value);
                    }
                })
                .keyBy(TrafficPageView::getSid)
                .process(new KeyedProcessFunction<String, TrafficPageView, TrafficPageView>()
                {
                    private ValueState<String> isFirst;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        isFirst = getRuntimeContext().getState(new ValueStateDescriptor<>("isFirst", String.class));
                    }

                    @Override
                    public void processElement(TrafficPageView value, Context ctx, Collector<TrafficPageView> out) throws Exception {

                        //说明当前是该会话中的第一次页面访问
                        if (isFirst.value() == null) {
                            value.setSvCt(1l);
                            isFirst.update("first");
                        }
                        out.collect(value);
                    }
                });


    }
}
