package cn.cud.edu.dws.paymentsuc.app.pojo;

import com.alibaba.fastjson.annotation.JSONField;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * author song
 * date 2025-01-16 16:59
 * Desc
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class PaySucWindowBean {

    @JSONField(serialize = false)
    private String userId;
    private String stt;
    private String edt;
    private String curDate;
    //添加dws表需要的指标字段
    private Long paymentSucUniqueUserCount = 0l;
    private Long paymentSucNewUserCount = 0l;
    @JSONField(serialize = false)
    private Long ts;
}
