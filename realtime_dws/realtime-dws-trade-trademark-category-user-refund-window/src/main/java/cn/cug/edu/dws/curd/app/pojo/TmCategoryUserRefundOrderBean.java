package cn.cug.edu.dws.curd.app.pojo;

import com.alibaba.fastjson.annotation.JSONField;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * author song
 * date 2025-01-16 18:20
 * Desc
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class TmCategoryUserRefundOrderBean {

    private String stt;
    private String edt;
    private String curDate;
    private String trademarkId;
    private String trademarkName;
    private String category1Id;
    private String category1Name;
    private String category2Id;
    private String category2Name;
    private String category3Id;
    private String category3Name;
    private String userId;
    /*
       orderId = 1, sku_id= A, 申请退单
       orderId = 1, sku_id= B,
       orderId = 1, sku_id= C, 申请退单

       口径：
          退单数:   要申请退货的商品所在的订单数。  1
          退单次数:  以同一笔订单的商品为粒度进行统计 2
     */
    private Long refundCount = 1l;

    //退单数据没有撤回
    //粒度： 一笔要退单的商品是一行
    //窗口计算，需要有时间。从dwd_refund_order中抽取
    @JSONField(serialize = false)
    private Long ts;

    //后续需要使用skuId关联维度
    private String skuId;
}
