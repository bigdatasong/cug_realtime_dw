package cn.cug.edu.common.util;

import java.util.ResourceBundle;

/**
 * author song
 * date 2024-08-03 16:28
 * Desc
 */
public class ConfigUtil {
    //传入properties文件的名字，不包括后缀 ResourceBundle看作是一个Map来用
   public static ResourceBundle config = ResourceBundle.getBundle("config");
    //编写方法读取config.properties的属性值
    public static String getString(String name){

        return config.getString(name);
    }

    public static Integer getInt(String name){

        return Integer.parseInt(config.getString(name));
    }

    public static void main(String[] args) {
        String kafkaBrokers = getString("KAFKA_BROKERS");
        System.out.println(kafkaBrokers);
    }
}
