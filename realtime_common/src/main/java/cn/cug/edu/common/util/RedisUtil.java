package cn.cug.edu.common.util;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

/**
 * author song
 * date 2025-01-16 18:22
 * Desc
 */
public class RedisUtil {

    private static JedisPool pool;

    static {
        //定制连接池的参数
        JedisPoolConfig jedisPoolConfig = new JedisPoolConfig();
        //池子的最大容量
        jedisPoolConfig.setMaxTotal(ConfigUtil.getInt("JEDIS_POOL_MAXTOTAL"));
        //池子中最小存活的连接数
        jedisPoolConfig.setMinIdle(ConfigUtil.getInt("JEDIS_POOL_MINIDLE"));
        //池子中最多存活的连接数
        jedisPoolConfig.setMaxIdle(ConfigUtil.getInt("JEDIS_POOL_MAXIDLE"));
        //阻塞时等待的最大时间，超过这个时间，依旧无法获取连接，就抛异常
        jedisPoolConfig.setMaxWaitMillis(ConfigUtil.getInt("JEDIS_POOL_MAXWAITMILLIS"));
        //客户端来借连接了，但是连接耗尽了，客户端要不要等一等(阻塞)
        jedisPoolConfig.setBlockWhenExhausted(true);
        //借连接时，先测试以下好使，再借
        jedisPoolConfig.setTestOnBorrow(true);
        //还连接时，先测试以下好使，还借
        jedisPoolConfig.setTestOnReturn(true);

        //创建一个连接池
        pool = new JedisPool(jedisPoolConfig,
                ConfigUtil.getString("JEDIS_POOL_HOST"),
                ConfigUtil.getInt("JEDIS_POOL_PORT"));

    }

    public static Jedis getJedis(){
        Jedis jedis = pool.getResource();
        jedis.select(ConfigUtil.getInt("JEDIS_DB_ID"));
        return jedis;
    }

    public static void close(Jedis jedis){
        if (jedis != null)
            //从池子中借的，此时是还回到池子，不是关闭
            jedis.close();
    }
}
