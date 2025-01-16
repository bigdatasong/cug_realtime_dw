package cn.cug.edu.dws.pageview.app.pojo;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * author song
 * date 2025-01-13 16:21
 * Desc
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class TrafficPageView {
    //要统计的指标所需要的字段
    private String mid;
    private Long duringTime;
    private String sid;
    //分组的维度
    private String vc;
    private String ar;
    private String ch;
    private String isNew;
    //提供dws表需要的其他字段
    //在做json解析时，只会把json中同名的字段的值赋值给你pojo
    private String stt;
    private String edt;
    private String curDate;
    //添加dws表需要的指标字段
    private Long uvCt = 0l;
    private Long svCt = 0l;
    private Long pvCt = 1l;
    private Long durSum = 0l;
    //添加时间 毫秒
    private long ts;
}
