package cn.cug.edu.common.constant;

/**
 * author song
 * date 2024-08-03 16:27
 * Desc
 */
public interface GmallConstant {

    //定义和新老客户有关标记
    String ISNEWOLD = "0";
    String ISNEWEW = "1";

    //接口中常量一定是static final类型
    //定义日志分流的属性名
    String START = "start";
    String ERR = "err";
    String DISPLAY = "displays";
    String PAGE = "page";
    String ACTION = "actions";

    //dwd日志相关
    String TOPIC_DWD_TRAFFIC_START = "dwd_traffic_start";
    String TOPIC_DWD_TRAFFIC_ERR = "dwd_traffic_err";
    String TOPIC_DWD_TRAFFIC_ACTION = "dwd_traffic_action";
    String TOPIC_DWD_TRAFFIC_DISPLAY = "dwd_traffic_display";
    String TOPIC_DWD_TRAFFIC_PAGE = "dwd_traffic_page";

    String TOPIC_DWD_INTERACTION_COMMENT_INFO = "dwd_interaction_comment_info";
    String TOPIC_DWD_TRADE_CART_ADD = "dwd_trade_cart_add";
    String TOPIC_DWD_TRADE_ORDER_DETAIL = "dwd_trade_order_detail";
    String TOPIC_DWD_TRADE_CANCEL_DETAIL = "dwd_trade_cancel_detail";
    String TOPIC_DWD_TRADE_PAY_DETAIL_SUC = "dwd_trade_pay_detail_suc";
    String TOPIC_DWD_TRADE_ORDER_REFUND = "dwd_trade_order_refund";
    String TOPIC_DWD_TRADE_REFUND_PAY_SUC = "dwd_trade_refund_pay_suc";
    String TOPIC_DWD_USER_REGISTER = "dwd_user_register";
    long SEVEN_DAY_MS = 7 * 24 * 60 * 60 * 1000;
    int TWO_DAY_SECONDS = 2 * 24 * 60 * 60;
}
