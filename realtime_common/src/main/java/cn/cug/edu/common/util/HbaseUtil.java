package cn.cug.edu.common.util;

import javafx.scene.AmbientLight;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import javax.swing.event.TableColumnModelEvent;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/**
 * author song
 * date 2024-08-05 21:31
 * Desc 实现对habse 的库和表的一系列操作
 *
 *      首先需要一个连接对象， 即connection ,habase的connection对象是线程安全的 一个app中只创建一次，多线程进行共享
 *      其次 对表的操作，比如对表的crud 创建表等操作需要一个admin对象
 *          对表数据的crud，插入数据等 就需要一个table对象 ，admin对象和table对象都是通过connection连接对象来获取得到，
 *          但是要注意创建一个表的话 就是需要通过admin对象的api createtable 来创建
 *          ，并且table和admin都是线程不安全的
 */
@Slf4j
public class HbaseUtil {

    // 所以我们需要一个connection对象 在类中要想保持对象只有一个 就必须是单例，可以通过静态代码块加静态属性的方式来获取
    public static Connection hbaseConnection = null;

    static {

        //habse的conncection对象可以通过connectionfactory来创建得到
        //创建数据库的连接对象  对于这里的异常的处理 ，之所以捕获是因为 已经时静态代码块了 ，所以如果直接又是抛出就会到jvm，所以jvm就会崩溃
        try {
            // 在这里我们使用的时空参的方法来创建 ，是因为底层会读取配置文件的zk ，从而不需要自己手动的创建hadoop的配置对象以及往配置对象中添加配置项
            //所以只需要在类路径下创建一个hbase-size.xml文件即可，里面放zk的地址
            //因为客户端连habse集群的原理就是，只需要知道hbase的元数据存放在哪里即可，而hbase的元数据是存放在zk中的
            //所以只需要在配置文件中放入zk的地址即可。
            hbaseConnection = ConnectionFactory.createConnection();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }


    //定义一个方法实现对连接的关闭
    public static void colseConnection(){
        // 如果连接不为null才需要关闭
        if (hbaseConnection != null){
            try {
                hbaseConnection.close();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    // 我们首先来定义一个创建库的方法，在hbase中库表示namespace的意思，有default 即默认库的意思，便于区分 我们一般在可以创建一个新库来使用
    // 创建库 需要用到admin来创建 ，而admin通过连接对象来创建
    // 因为table对象也需要通过admin来 不妨我们先定义一个方法 来获取admin对象，因为admin对象在多个地方都会用到所以抽取出一个方法

    //先创建一个能够得到admin对象的方法
    public static Admin getAdmin() throws IOException {
        Admin admin = hbaseConnection.getAdmin();

        return admin;
    }

    // 然后开始创建一个能够创建库的方法 参数就是库名以及admin对象传入
    //但是在创建库之前 同样也要知道这个库是否存在 所以需要在创建之前再编写一个判断库是否存在的方法

    // 所以先编写一个库是否存在的方法 参数就是admin 以及 库名
    public static Boolean checkNameSpaceExist(Admin admin,String nameSpace){
        //首先需要对参数进行判断是否合法
        //通过isanyblank来判断，只要可变参数有一个非法的就返回true 注意是或的关系
        if (StringUtils.isAnyBlank(nameSpace)) {
            //返回true说明他是非法的参数，那么就可以抛异常 或者说通过日志来打印
            throw  new RuntimeException("库名非法！！！");
        }else {
            // 通过admin的getNamespaceDescriptor 来判断如果说通过库名 没有得到库描述的话 就会抛出异常
            //那么我们可以通过是否有异常来返回true或者false；
            try {
                admin.getNamespaceDescriptor(nameSpace);
                return true;
            } catch (IOException e) {
                return false;
            }
        }
    }

    //这个才是真正创建库的方法
    public static void createNameSpace(Admin admin,String nameSpace) throws IOException {

        // 首先需要判断库是否存在如果存在就不需要创建了 直接抛异常
        Boolean aBoolean = checkNameSpaceExist(admin, nameSpace);
        if (aBoolean) {
            //说明库存在
            throw new RuntimeException("库已经存在！！！");
        }
        // 不用else也可以，因为如果上面走不了 就直接抛异常 程序就直接停止了 所以肯定不会走下面的代码
        // 其次判断库名是否合法
        if (StringUtils.isAnyBlank(nameSpace)) {
            // 直接抛异常
            throw new RuntimeException("库名非法！！！！");
        }

        //接下来才可以调用admin的方法来创建库
        //参数为 NamespaceDescriptor对象 但是不能通过new 的方式 因为他的构造器是私有的，所以需要看它是否有静态方法
        //在NamespaceDescriptor中有create的静态方法 但他的返回值是build类， build类中有build方法可以构建出NamespaceDescriptor对象
        admin.createNamespace(NamespaceDescriptor.create(nameSpace).build());
    }

    //还需要封装一个返回tablename对象，这个对象在多个地方都需要
    // tablename对象通过 通过库名加表名来确定一个tablename对象
    public static TableName gettbName(String ns, String tb){
        //首先需要判断参数是否合法
        if (StringUtils.isAnyBlank(ns,tb)) {
            //说明参数不合法 可以直接抛异常
            throw new RuntimeException("表名或者库名不合法");
        }
        //然后返回tablename对象  tablename有静态方法valueof 一个参数就必须库名:表名 两个参数的话就可以分开
        TableName tableName = TableName.valueOf(ns, tb);
        return tableName;
    }

    //接下来就是创建表的方法 ，在创建表之前还需要一个判断表是否存在的方法
    public static Boolean isTableExist(Admin admin,String ns,String tbName) throws IOException {
        // 因为tableExists方法参数是tableName ，而构建一个tablename 需要以库名 + 表名 ，所以参数需要库名
        boolean b = admin.tableExists(TableName.valueOf(ns, tbName));
        return b;

    }

    //接下来就是定义创建表的方法
    //对于表的创建我们需要 提供namespace table名 列族 可以有多个列族 ，也即创建表的话，需要到列族，并且至少要有一个列族
    public static void createTable(Admin admin,String ns,String tbName,String...columnFamiles) throws IOException {
        //先检查参数是否合法
        if (columnFamiles.length < 1 || StringUtils.isAnyBlank(ns,tbName)){
            //说明参数不合法
            throw new RuntimeException("参数不合法");
        }
        // 然后分别检查库和表是否已经存在
        if (!checkNameSpaceExist(admin,ns)) {
            //库不存在就需要创建库
            createNameSpace(admin,ns);
        }

        if (!isTableExist(admin,ns,tbName)) {
            // 表不存在就需要开始创建
            // 在调用创建表的api的方法中 需要一个TableDescriptor 对象
            //TableDescriptor对象是一个接口 并且构造器私有，需要通过TableDescriptorBuilder对象来构造
            //TableDescriptorBuilder中有很多重要的方法，其中
            // build方法可以返回一个TableDescriptor，那么在build返回之前肯定需要将有关表的参数进行封装的，
            //作为TableDescriptor 即一个表描述对象，肯定需要通过库名加表名来确定一个表
            //所以在TableDescriptorBuilder中有一个newbuilder方法 其返回还是TableDescriptorBuilder，方法参数是一个tablename，而正好
            //一个tablename对象就对应一个表
            //表确定好以后 需要确定列族 同样也是通过TableDescriptorBuilder中的setColumnFamilies或者setColumnFamily来确定
            //方法的区别在于参数一个是ColumnFamilyDescriptor的集合或者单个ColumnFamilyDescriptor，
            //ColumnFamilyDescriptor 对应列的描述符，如果说你有多个列的话可以直接封装成一个集合。然后直接set即可。
            //ColumnFamilyDescriptor同样需要通过ColumnFamilyDescriptorBuilder类来得到
            //也即在ColumnFamilyDescriptorBuilder中有build方法就可以得到一个ColumnFamilyDescriptor
            //同样在build前需要给列族描述符来赋值参数
            //在ColumnFamilyDescriptorBuilder中有两种方式来赋值列族参数分别是of方法以及newbuilder方法
            //二者返回是不一样的 其中of方法直接可以得到一个ColumnFamilyDescriptor，而后者就返回的是ColumnFamilyDescriptorBuilder，然后再通过build方法就可以得到ColumnFamilyDescriptor
            //另外参数也是有不同的，of方法中直接传递一个字节数组，即将stirng的列族直接通过工具类变成字节数组即可，
            //而newbuilder方法可以直接传递一个string 或者传递一个字节数组也是可以的
            //字节数组工具类是Bytes 实现常见类型和字节数组的转换，tobytes 将常见的数据类型转为字节数组 toT 将字节数组转为常见的数据类型

            // 可以看到我们需要一个ColumnFamilyDescriptor集合 因为我们是可变数组，所以可以直接给一个集合
            //思路就是将列族名封装成对应一个ColumnFamilyDescriptor 然后变成一个集合传递进去即可
            //集合操作通过streamapi操作
            List<ColumnFamilyDescriptor> columnFamilyDescriptors = Arrays.stream(columnFamiles).map(column -> ColumnFamilyDescriptorBuilder.of(column)).collect(Collectors.toList());

            // 或者
            // List<ColumnFamilyDescriptor> collect = Arrays.stream(columnFamiles).map(col -> ColumnFamilyDescriptorBuilder.of(Bytes.toBytes(col))).collect(Collectors.toList());
            //或者
            // List<ColumnFamilyDescriptor> collect = Arrays.stream(columnFamiles).map(col -> ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes(col)).build()).collect(Collectors.toList());

            admin.createTable(TableDescriptorBuilder.newBuilder(gettbName(ns,tbName)).setColumnFamilies(columnFamilyDescriptors).build());
        }
    }

    //实现对表的删除
    public static Boolean dropTable(Admin admin,String ns,String tbName) throws IOException {

        // 先判断表是否存在 表存在就不需要删除
        if (!isTableExist(admin,ns,tbName)){
            log.warn(tbName + "这个表存在");
            return false;
        }
        //删除表前需要先移除表
        admin.disableTable(gettbName(ns,tbName));
        admin.deleteTable(gettbName(ns,tbName));
        return true;
    }

    // 接下来来开始封装对表数据的crud，需要一个table对象，所以可以提前封装方法实现table对象的返回，
    //还有就是插入数据时，put 表示的是对单行的插入，get表示的是对单行的查询，scan 表示的是对多行的查询
    //put 或者get 一行都需要 精确到列值 即 表名 rowkey 列族名：列名 列值
    //另外因为put对象是比较难封装的，所以也可以定义一个方法来返回put对象

    // 实现table对象的返回
    public static Table getTable(String ns, String tb) throws IOException {
        // 同样先判断参数是否合法
        if (StringUtils.isAnyBlank(ns,tb)) {
            //说明参数不合法
            throw new RuntimeException("表名或者库名不合法！！");
        }

        // 调用connection的方法
        Table table = hbaseConnection.getTable(gettbName(ns, tb));
        return table;
    }

    // 实现对put对象的返回 参数分别是rowkey 列族 列名 列值
    public static Put getPut(String rk,String cf,String cn,String value){

        //判断参数是否合法
        if (StringUtils.isAnyBlank(rk,cf,cn,value)) {
            throw new RuntimeException("参数不合法！！");
        }
        //封装put对象 一个put表示的是对单行的操作 即对应单行 而rowkey就表示一个单行的意思
        Put put = new Put(Bytes.toBytes(rk));
        // 还需要封装其他的参数 分别是列族 列名 列值
        put.addColumn(Bytes.toBytes(cf), Bytes.toBytes(cn),Bytes.toBytes(value));
        return put;

    }

}
