package cn.cud.edu.dws.paymentsuc.app;

import cn.cud.edu.dws.paymentsuc.app.pojo.PaySucWindowBean;
import cn.cug.edu.common.base.BaseDataStreamApp;
import cn.cug.edu.common.constant.GmallConstant;
import cn.cug.edu.common.function.DorisMapFunction;
import cn.cug.edu.common.util.DateFormatUtil;
import cn.cug.edu.common.util.DorisUtil;
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
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * author song
 * date 2025-01-16 16:43
 * Desc
 */
public class DwsTradePaymentSucWindow extends BaseDataStreamApp {

    public static void main(String[] args) {
        new DwsTradePaymentSucWindow().start(
                13007,4, GmallConstant.DWS_TRADE_PAYMENT_SUC_WINDOW,GmallConstant.TOPIC_DWD_TRADE_PAY_DETAIL_SUC
        );
    }
    @Override
    protected void hadle(StreamExecutionEnvironment env, DataStreamSource<String> kfSource) {

        //读数据，封装为pojo
        SingleOutputStreamOperator<PaySucWindowBean> pojoDs = parseToPojo(kfSource);
        //开窗聚合
        SingleOutputStreamOperator<PaySucWindowBean> aggDs = agg(pojoDs);
        //写出到Doris
        writeToDoris(aggDs);


    }

    private void writeToDoris(SingleOutputStreamOperator<PaySucWindowBean> aggDs) {
        aggDs
                .map(new DorisMapFunction<>())
                .sinkTo(DorisUtil.getDorisSink("cug_rt_gmall.dws_trade_payment_suc_window"));
    }

    private SingleOutputStreamOperator<PaySucWindowBean> agg(SingleOutputStreamOperator<PaySucWindowBean> pojoDs) {

        WatermarkStrategy<PaySucWindowBean> watermarkStrategy = WatermarkStrategy
                .<PaySucWindowBean>forMonotonousTimestamps()
                .withTimestampAssigner( (e, ts) -> e.getTs() * 1000)
                .withIdleness(Duration.ofSeconds(10));

        return  pojoDs
                .assignTimestampsAndWatermarks(watermarkStrategy)
                .windowAll(TumblingEventTimeWindows.of(Time.seconds(5)))
                .reduce(new ReduceFunction<PaySucWindowBean>()
                {
                    @Override
                    public PaySucWindowBean reduce(PaySucWindowBean value1, PaySucWindowBean value2) throws Exception {
                        value1.setPaymentSucNewUserCount(value1.getPaymentSucNewUserCount() + value2.getPaymentSucNewUserCount());
                        value1.setPaymentSucUniqueUserCount(value1.getPaymentSucUniqueUserCount() + value2.getPaymentSucUniqueUserCount());
                        return value1;
                    }
                }, new AllWindowFunction<PaySucWindowBean, PaySucWindowBean, TimeWindow>()
                {
                    @Override
                    public void apply(TimeWindow window, Iterable<PaySucWindowBean> values, Collector<PaySucWindowBean> out) throws Exception {
                        PaySucWindowBean result = values.iterator().next();
                        result.setStt(DateFormatUtil.tsTodateTime(window.getStart()));
                        result.setEdt(DateFormatUtil.tsTodateTime(window.getEnd()));
                        result.setCurDate(DateFormatUtil.tsToDate(window.getStart()));
                        out.collect(result);
                    }
                });

    }

    private SingleOutputStreamOperator<PaySucWindowBean> parseToPojo(DataStreamSource<String> ds) {

        return  ds
                .map(s -> JSON.parseObject(s, PaySucWindowBean.class))
                .keyBy(PaySucWindowBean::getUserId)
                .process(new KeyedProcessFunction<String, PaySucWindowBean, PaySucWindowBean>()
                {
                    private ValueState<String> lastPayDate;
                    //标准化处理，判断当前这条数据是否在今天下过单，是否在之前下过单

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        lastPayDate = getRuntimeContext().getState(new ValueStateDescriptor<>("lastPayDate", String.class));
                    }

                    @Override
                    public void processElement(PaySucWindowBean value, Context ctx, Collector<PaySucWindowBean> out) throws Exception {
                        String lastPayDateStr = lastPayDate.value();
                        //获取业务日期
                        String eventDate = DateFormatUtil.tsToDate(value.getTs() * 1000);

                    /*
                        情形一: lastPayDateStr=null,当前用户之前从未支付成功过，这是第一次支付，也是今天的第一次支付
                        情形二: lastPayDateStr!=null,当前用户今天从未支付成功过，这是今天的第一次支付
                     */
                        if (!eventDate.equals(lastPayDateStr)) {
                            value.setPaymentSucUniqueUserCount(1l);
                            if (lastPayDateStr == null) {
                                value.setPaymentSucNewUserCount(1l);
                            }
                            lastPayDate.update(eventDate);
                        }

                        if (value.getPaymentSucUniqueUserCount() > 0) {
                            out.collect(value);
                        }
                    }
                });

    }
}
