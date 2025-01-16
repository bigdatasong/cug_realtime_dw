import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * author song
 * date 2024-08-16 15:27
 * Desc
 * 在flinksql中无论是tableapi 还是sqlapi 都需要一个动态表对象即Table，
 * 环境的建立：对于flinksql中的环境需要一个tableEnvironment 环境对象，他是一个接口，
 *           这个环境对象有两种构造方式：第一个就是通过流的环境来构造
 *                         因为tableEnvironment他是一个接口，并且它有一个子接口StreamTableEnvironment
 *                         子接口有一个create方法，该方法的参数就是流的env，并且返回一个StreamTableEnvironment
 *           StreamTableEnvironment streamTableEnvironment = StreamTableEnvironment.create(流的env);
 *
 *           第二种就是不需要流的环境 而是直接构造一个tableEnvironment
 *             TableEnvironment tableEnvironment = TableEnvironment.create();
 *
 * table对象的创建
 *    可以通过sqlapi的方式来创建，也可以通过流 来得到table对象
 *
 *    1. 流的方式来创建
 *    一般来说我们会有一个流， 那么就可以通过
 *      StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
 *      StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
 *       Table table = tableEnv.fromDataStream() 参数第一个可以是一个流，第二个可以是schema 这个在sql中定义时间就是在这里定义
 *     因为在sql中流也有很多种：新增流、撤回流等，其中fromDataStream种需要的就是流就是需要一个新增流
 *
 *    2. 通过sqlapi的方式创建的话就是
 *    tableEnv中的sqlQuery()方法 返回的就是一个 table对象
 *
 *   tips: 在tableEnv中有两个api比较重要，一个就是executsql（） 另一个就是sqlquery（），
 *         executsql（）一般来说就是执行某个sql语句要使用，就是比如说创建表的sql语句 或者说插入语句 不需要返回值，但是它是有返回值的
 *         他的返回类型就是Tableresult
 *         sqlquery 就是执行查询语句地时候使用，返回的是一个table对象 也即对动态表执行操作以后返回还是一个table表对象
 *
 *         在table对象中有几个方法比较重要
 *         1.table.printSchema() ：即打印出当前动态表结构
 *         2. table。execute（）: 表示的是对动态表执行 返回的是一个tableresult对象，
 *                 所以说一般我们在使用的时候有两种方式来输出
 *                  1. tableEnv.executesql().print()
 *                  2. tableEnv.sqlquery().execute().print() 两者都是等价的 都是通过tableresult对象的print来输出。
 *
 *       还有就是为表取别名，什么时候需要为表取别名呢，一般来说通过流得到的表对象是需要取别名的，因为如果说直接通过sqlquery来得到表对象的话
 *       他里面的sql语句就已经有表的名字了
 *          既然说通过流得到表对象需要取别名的话 也有两种方式来取别名
 *          1. tableEnv.createTemporaryView() 他是一个重载的方法
 *              一般来说可以第一个参数都是表别名，第二个参数可以是一个流，即省去了从流得到表对象的过程
 *                                          或者第二个参数可以是一个表对象，即如果有时候表对象先通过流创建出来的话就可以
 *                                          传一个表对象进去也是可以的。
 *
 */
public class BaseTest1 {

    public static void main(String[] args) {




    }
}
