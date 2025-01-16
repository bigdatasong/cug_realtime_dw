package cn.cug.edu.dwd.comment.app;

import cn.cug.edu.common.base.BaseStreamTableApp;
import cn.cug.edu.common.constant.GmallConstant;
import cn.cug.edu.common.util.SqlUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * author song
 * date 2024-08-16 14:14
 * Desc 处理互动域的评论表 事务事实表
 *      在业务数据库中有一张comment表 记录了 评论这一事实，即记录了谁 评论了某一商品
 *      dwd层的目的就是要描述一个事实 并且进行维度退化，就是让信息更丰富一些，比如说这张表中有一个
 *      appraise字段 他只记录了编码信息，而应该把它实际的意思给得到，就需要和一个编码表进行关联，
 *      就涉及到join了 在flink中涉及到多个表的关联 如果业务逻辑不复杂一般都是使用finksql，
 *      而我们这里的需求就是只需要补充一个字段即可，所以可以使用flinksql的join 。而又因为那个编码表
 *      在habse中，所以就是维度关联，对于事实表的数据量大，而维度信息基本上不会改变的情况下，就使用look-up join即可
 *
 *      对于评论这个业务事实 我们只关心insert操作，为啥呢，首先用户可以对某个东西进行评论后就会在表中插入一条数据，也有可能删除一条评论
 *      一般来说 评论表的数据是很大的，所以我们一般都是直接增加，不会去修改之前的评论，所以我们只关心insert操作的数据
 *
 *      因为涉及到flinksql编程 之前baseapp只有流式编程 所以我们也可以定义一个sql的编程base。
 *
 *      编写号table的baseapp以后就应该通过具体的需求 来继承这个baseapp 比如这个commentinfo来继承它
 */
public class DwdinteractionCommentInfo extends BaseStreamTableApp {

    public static void main(String[] args) {
        new DwdinteractionCommentInfo().start(
                11002,
                4,
                GmallConstant.TOPIC_DWD_INTERACTION_COMMENT_INFO
        );
    }
    @Override
    protected void handle(StreamTableEnvironment tableEnv, StreamExecutionEnvironment env) {
        //1.首先我们就需要读取数据 dwd需要的数据都在db的topic中，所以我们需要sql的方式来读取一个kafkasource 即需要通过一个kafka的连接器来读取
        //因为需要创建一个表来对应kafkasource 在具体的语法中基本上都有一些连接器的参数 所以我们需要一个工具类sqlutils来定义一些连接器的参数
        //在sqlutils中编写好连接kafkasource参数后，需要在baseapp中定义一个方法，来实现创建一个表来对应这个kafkasource 表对象
        //这个方法是用于子类来调用的

        // 创建ods表
        createOdsDB(tableEnv,GmallConstant.TOPIC_DWD_INTERACTION_COMMENT_INFO);

        // 测试创建的ods_db 表 打印其表结构
       // tableEnv.sqlQuery("select * from ods_db").printSchema();

       // tableEnv.sqlQuery("select * from ods_db").execute().print();

        //创建好源数据和表对应以后 就有了数据 但是这只是对所有数据的映射 接下来就需要过滤出事comment 评论表的数据
        //过滤的思路就是从创建好的ods_db中过滤出需要的字段，然后和维度表中的数据进行join 最后写出到kafka中。
        //定义过滤commentinfo的sql语句
        //首先需要的字段都在ods_db中的data中并且去除掉了一些不需要的字段
        String commentInfoSql = "select " +
                "  data['id'] id, " +
                "  data['user_id'] user_id, " +
                "  data['sku_id'] sku_id, " +
                "  data['spu_id'] spu_id, " +
                "  data['order_id'] order_id, " +
                "  data['appraise'] appraise, " +
                "  data['comment_txt'] comment_txt , " +
                "  data['create_time'] create_time ,  " +
                "   ts ,  " +
                "   pt   " +
                " from ods_db " +
                " where `database` = 'cug_rt_gmall'" +
                " and `table` = 'comment_info'" +
                " and `type` = 'insert' ";

        //通过tableevn执行语句就可以得到需要的commentinfo
        Table commentInfo = tableEnv.sqlQuery(commentInfoSql);
        tableEnv.createTemporaryView("commentInfo",commentInfo);

     //   commentInfo.printSchema();


        //此时需要准备好维度数据 因为需要和维度数据进行关联 从而丰富维度数据，同样因为维度数据在hbase中，如果通过
        //flinksql去读取hbase数据的话 同样也需要连接器参数 同样需要映射表 所以需要在sqlutil中加连接habse的连接器参数 并且还需要在
        //baseapp中创建一个映射hbase的方法 供子类调用  连接hbase需要加依赖

        //3.创建一个look-up表，映射hbase中的   dim_dic_code
        createDicCode(tableEnv);

       // tableEnv.sqlQuery("select * from dim_dic_code").printSchema();


        //4.进行look-upjoin
        //要求事实表中必须有处理时间
        String joinSql =" SELECT commentInfo.id,user_id,sku_id,spu_id,order_id,appraise , dim_dic_code.info.dic_name," +
                "  comment_txt, create_time, ts " +
                " FROM commentInfo  " +
                "  left JOIN dim_dic_code FOR SYSTEM_TIME AS OF commentInfo.pt " +
                "    ON commentInfo.appraise = dim_dic_code.id; ";

       // tableEnv.sqlQuery(joinSql).printSchema();





        //5.创建一个sink表，把需要的字段写出到kafka
        String sinkSql = " create table " + GmallConstant.TOPIC_DWD_INTERACTION_COMMENT_INFO +"(" +
                "   id STRING, " +
                "   user_id STRING, " +
                "   sku_id STRING, " +
                "   spu_id  STRING, " +
                "   order_id STRING, " +
                "   appraise STRING, " +
                "   appraiseName STRING, " +
                "   comment_txt STRING, " +
                "   create_time STRING,  " +
                "   ts BIGINT " + SqlUtil.getKafkaSinkSql(GmallConstant.TOPIC_DWD_INTERACTION_COMMENT_INFO );

        tableEnv.executeSql(sinkSql);

//        tableEnv.executeSql(sinkSql).print();
//
        tableEnv.executeSql("insert into " + GmallConstant.TOPIC_DWD_INTERACTION_COMMENT_INFO +  joinSql);
//
//
//





    }
}



