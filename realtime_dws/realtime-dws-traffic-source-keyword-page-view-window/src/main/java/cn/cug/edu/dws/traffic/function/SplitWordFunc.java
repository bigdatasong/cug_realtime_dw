package cn.cug.edu.dws.traffic.function;

import cn.cug.edu.common.util.ConfigUtil;
import cn.cug.edu.dws.traffic.util.IKUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

import java.util.Set;

/**
 * author song
 * date 2025-01-16 19:04
 * Desc
 */
@FunctionHint(output = @DataTypeHint("ROW<word STRING>"))
public  class SplitWordFunc extends TableFunction<Row>
{
    /*
        提供多个名字为 eval()的方法
            方法不能有返回值。
            参数列表可以任意
            调用collect进行输出，每次调用输出一行。
     */
    public void eval(String str){
        if (StringUtils.isBlank(str)){
            //不输出
            return ;
        }else {
            Set<String> words = IKUtil.splitWord(str, ConfigUtil.getBooleanValue("SPLIT_WORD_MODE"));
            for (String word : words) {
                collect(Row.of(word));
            }
        }
    }
}