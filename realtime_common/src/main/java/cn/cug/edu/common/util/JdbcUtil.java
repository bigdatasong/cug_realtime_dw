package cn.cug.edu.common.util;

import cn.cug.edu.common.pojo.TableProcess;
import com.alibaba.druid.pool.DruidDataSource;
import com.alibaba.druid.pool.DruidPooledConnection;
import com.mysql.cj.x.protobuf.MysqlxCursor;
import org.apache.commons.dbutils.BasicRowProcessor;
import org.apache.commons.dbutils.QueryRunner;
import org.apache.commons.dbutils.handlers.BeanListHandler;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;

/**
 * author song
 * date 2024-08-08 17:16
 * Desc  使用dbutils来操作数据库
 */
public class JdbcUtil {

    //首先肯定需要一个连接 使用连接池的方式来创建连接
    public static DruidDataSource dataSource;

    //通过静态代码块的方式来创建池
    static {
        // 初始化连接池配置
        dataSource = new DruidDataSource();
        dataSource.setUrl(ConfigUtil.getString("MYSQL_URL"));
        dataSource.setUsername(ConfigUtil.getString("MYSQL_USER"));
        dataSource.setPassword(ConfigUtil.getString("MYSQL_PASSWORD"));

        // 其他连接池配置...
        dataSource.setInitialSize(ConfigUtil.getInt("POOL_INITIAL_SIZE")); // 初始化连接数
        dataSource.setMaxActive(ConfigUtil.getInt("POOL_MAX_ACTIVE")); // 最大连接数
        dataSource.setMinIdle(ConfigUtil.getInt("POOL_MIN_IDLE")); // 最小空闲连接数
        dataSource.setMaxWait(ConfigUtil.getInt("POOL_MAX_WAITTIME")); // 获取连接的最大等待时间，单位毫秒
    }

    //定义一个通过池子来创建连接的方法
    public static Connection getconnection() throws SQLException {
        DruidPooledConnection connection = dataSource.getConnection();
        return connection;

    }

    //定义关闭连接的方法
    public static void CloConn(Connection connection) throws SQLException {

        if (connection != null){

            connection.close();
        }

    }

    //定义一个查询方法 通过一个dbutils工具类中的queryrunner类来进行查询
    //这个方法返回的是一个list 是将表中所有数据进行返回 并且封装成为一个list 并且泛型不固定类型
    public static <T> List<T> queryList(String sql,Class<T> t) throws SQLException {

        QueryRunner queryRunner = new QueryRunner();
        // resultsethandler中有很多的子类，其中BeanListHandler表示的通过一个sql
        //语句返回一个list集合 list集合的每一个元素都可以指定一个bean的类型返回一个bean
        List<T> query = queryRunner.query(JdbcUtil.getconnection(), sql, new BeanListHandler<>(t, new BasicRowProcessor()));

        return query;

    }



}
