package cn.cug.edu.dws.userLogin.app.bean;

import com.alibaba.fastjson.annotation.JSONField;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * author song
 * date 2025-01-14 15:38
 * Desc
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class UserLoginBean {

    //用户id
    @JSONField(serialize = false)
    String uid;
    //上一个页面id
    @JSONField(serialize = false)
    String last_page_id;
    // 窗口起始时间
    String stt;
    // 窗口终止时间
    String edt;
    String curDate;
    // 回流用户数
    Long backCt = 0l;
    // 独立用户数
    Long uuCt =0l;
    // 时间戳
    @JSONField(serialize = false)
    Long ts;
}
