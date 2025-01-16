package cn.cug.edu.dws.app.bean;

import com.alibaba.fastjson.annotation.JSONField;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * author song
 * date 2025-01-14 14:48
 * Desc
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class HomeDetailPVBean {

    @JSONField(serialize = false)
    private String mid;
    @JSONField(serialize = false)
    private String pageId;

    private String stt;
    private String edt;
    private String curDate;
    //添加dws表需要的指标字段
    private Long home_uv_ct = 0l;
    private Long good_detail_uv_ct = 0l;
    @JSONField(serialize = false)
    private Long ts;


}
