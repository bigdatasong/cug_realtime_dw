package cn.cug.edu.common.util;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

/**
 * author song
 * date 2024-08-15 14:14
 * Desc 因为日志中的ts是毫秒 即11位，需要将ts和日期格式的string相互转换
 */
public class DateFormatUtil {

    // 先定义日期格式 两种分别是日期+时间 和日期格式 两者的区别在于前者有时分秒 后者只到年月日
    private static final DateTimeFormatter dateformatter = DateTimeFormatter.ofPattern("yyyy-MM-dd");
    private static final DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    // 将ts转为日期+时间
    public static String tsTodateTime(Long ts){
        String stringDateTime = LocalDateTime.ofInstant(Instant.ofEpochMilli(ts), ZoneId.systemDefault()).format(dateTimeFormatter);

        return stringDateTime;

    }

    //将ts转为日期
    public static String tsToDate(Long ts){
        String stringDate = LocalDateTime.ofInstant(Instant.ofEpochMilli(ts), ZoneId.systemDefault()).format(dateformatter);

        return stringDate;

    }
    // 将日期+时间转为 ts
    public static Long dateTimeToTs(String dateTime){
        long ts = LocalDateTime.parse(dateTime, dateTimeFormatter).atZone(ZoneId.systemDefault()).toInstant().toEpochMilli();

        return ts;
    }

    // 将日期 转为ts
    public static Long dateToTs(String date ){
        //也可以使用localdate来解析 只不过使用localdate解析以后还需要转为localdatetime对象
        long ts = LocalDate.parse(date, dateformatter)
                .atStartOfDay(ZoneId.systemDefault())
                .toInstant()
                .toEpochMilli();

        return ts;
    }
}

