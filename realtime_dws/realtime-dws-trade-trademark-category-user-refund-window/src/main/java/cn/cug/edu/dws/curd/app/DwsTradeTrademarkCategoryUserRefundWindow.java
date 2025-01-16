package cn.cug.edu.dws.curd.app;

import cn.cug.edu.common.base.BaseDataStreamApp;
import cn.cug.edu.common.constant.GmallConstant;
import cn.cug.edu.common.function.DorisMapFunction;
import cn.cug.edu.common.function.LookUpJoinFunction;
import cn.cug.edu.common.util.DateFormatUtil;
import cn.cug.edu.common.util.DorisUtil;
import cn.cug.edu.dws.curd.app.pojo.TmCategoryUserRefundOrderBean;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * author song
 * date 2025-01-16 18:18
 * Desc
 */
public class DwsTradeTrademarkCategoryUserRefundWindow extends BaseDataStreamApp {
    public static void main(String[] args) {
        new DwsTradeTrademarkCategoryUserRefundWindow().start(
                13111,4, GmallConstant.DWS_TRADE_TRADEMARK_CATEGORY_USER_REFUND_WINDOW,GmallConstant.TOPIC_DWD_TRADE_ORDER_REFUND
        );
    }
    @Override
    protected void hadle(StreamExecutionEnvironment env, DataStreamSource<String> kfSource) {

        //根据skuId,关联维度，封装数据为POJO
        SingleOutputStreamOperator<TmCategoryUserRefundOrderBean> pojoDs = parseToPojo(kfSource);
        //关联维度
        SingleOutputStreamOperator<TmCategoryUserRefundOrderBean> dimOpDs = getDimData(pojoDs);
        //dimOpDs.print();
        //开窗聚合
        SingleOutputStreamOperator<TmCategoryUserRefundOrderBean> aggDs = agg(dimOpDs);
        //写出
        writeToDoris(aggDs);


    }

    private SingleOutputStreamOperator<TmCategoryUserRefundOrderBean> getDimData(SingleOutputStreamOperator<TmCategoryUserRefundOrderBean> pojoDs) {
        return
                pojoDs
                        //使用sku_id 关联 sku_info
                        .map(new LookUpJoinFunction<TmCategoryUserRefundOrderBean>("sku_info")
                        {
                            @Override
                            public String getIdValue(TmCategoryUserRefundOrderBean value) {
                                return value.getSkuId();
                            }

                            @Override
                            protected void extractDimData(TmCategoryUserRefundOrderBean value, JSONObject dimData) {
                                value.setCategory3Id(dimData.getString("category3_id"));
                                value.setTrademarkId(dimData.getString("tm_id"));
                            }
                        })
                        .map(new LookUpJoinFunction<TmCategoryUserRefundOrderBean>("base_trademark")
                        {
                            @Override
                            public String getIdValue(TmCategoryUserRefundOrderBean value) {
                                return value.getTrademarkId();
                            }

                            @Override
                            protected void extractDimData(TmCategoryUserRefundOrderBean value, JSONObject dimData) {
                                value.setTrademarkName(dimData.getString("tm_name"));
                            }
                        })
                        .map(new LookUpJoinFunction<TmCategoryUserRefundOrderBean>("base_category3")
                        {
                            @Override
                            public String getIdValue(TmCategoryUserRefundOrderBean value) {
                                return value.getCategory3Id();
                            }

                            @Override
                            protected void extractDimData(TmCategoryUserRefundOrderBean value, JSONObject dimData) {
                                value.setCategory3Name(dimData.getString("name"));
                                value.setCategory2Id(dimData.getString("category2_id"));
                            }
                        })
                        .map(new LookUpJoinFunction<TmCategoryUserRefundOrderBean>("base_category2")
                        {
                            @Override
                            public String getIdValue(TmCategoryUserRefundOrderBean value) {
                                return value.getCategory2Id();
                            }

                            @Override
                            protected void extractDimData(TmCategoryUserRefundOrderBean value, JSONObject dimData) {
                                value.setCategory2Name(dimData.getString("name"));
                                value.setCategory1Id(dimData.getString("category1_id"));
                            }
                        })
                        .map(new LookUpJoinFunction<TmCategoryUserRefundOrderBean>("base_category1")
                        {
                            @Override
                            public String getIdValue(TmCategoryUserRefundOrderBean value) {
                                return value.getCategory1Id();
                            }

                            @Override
                            protected void extractDimData(TmCategoryUserRefundOrderBean value, JSONObject dimData) {
                                value.setCategory1Name(dimData.getString("name"));

                            }
                        });
    }

    private void writeToDoris(SingleOutputStreamOperator<TmCategoryUserRefundOrderBean> aggDs) {
        aggDs
                .map(new DorisMapFunction<>())
                .sinkTo(DorisUtil.getDorisSink("cug_rt_gmall.dws_trade_trademark_category_user_refund_window"));
    }

    private SingleOutputStreamOperator<TmCategoryUserRefundOrderBean> agg(SingleOutputStreamOperator<TmCategoryUserRefundOrderBean> pojoDs) {
        return pojoDs
                .keyBy(b -> b.getTrademarkId() + "_" + b.getCategory3Id() + "_" + b.getUserId())
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                .reduce(new ReduceFunction<TmCategoryUserRefundOrderBean>()
                        {
                            @Override
                            public TmCategoryUserRefundOrderBean reduce(TmCategoryUserRefundOrderBean value1, TmCategoryUserRefundOrderBean value2) throws Exception {
                                value1.setRefundCount(value1.getRefundCount() + value2.getRefundCount());
                                return value1;
                            }
                        },
                        new ProcessWindowFunction<TmCategoryUserRefundOrderBean, TmCategoryUserRefundOrderBean, String, TimeWindow>()
                        {
                            @Override
                            public void process(String key, Context context, Iterable<TmCategoryUserRefundOrderBean> elements, Collector<TmCategoryUserRefundOrderBean> out) throws Exception {
                                TmCategoryUserRefundOrderBean result = elements.iterator().next();
                                TimeWindow window = context.window();
                                result.setStt(DateFormatUtil.tsTodateTime(window.getStart()));
                                result.setEdt(DateFormatUtil.tsTodateTime(window.getEnd()));
                                result.setCurDate(DateFormatUtil.tsToDate(window.getStart()));
                                out.collect(result);
                            }
                        });
    }

    private SingleOutputStreamOperator<TmCategoryUserRefundOrderBean> parseToPojo(DataStreamSource<String> ds) {

        WatermarkStrategy<TmCategoryUserRefundOrderBean> watermarkStrategy = WatermarkStrategy
                .<TmCategoryUserRefundOrderBean>forMonotonousTimestamps()
                .withTimestampAssigner( (e, ts) -> e.getTs() * 1000)
                .withIdleness(Duration.ofSeconds(10));

        return  ds
                .map(s -> JSON.parseObject(s, TmCategoryUserRefundOrderBean.class))
                .assignTimestampsAndWatermarks(watermarkStrategy);


    }


}
