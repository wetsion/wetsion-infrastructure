package site.wetsion.framework.infrastucture.cache;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.JedisSentinelPool;
import redis.clients.util.Pool;

import javax.annotation.PreDestroy;
import java.util.LinkedHashSet;
import java.util.Set;

/**
 * @author 霜华
 * @date 2021/6/30 6:02 PM
 **/
public class RedisJedisPool {

    private static final Logger log = LoggerFactory.getLogger(RedisJedisPool.class);

    JedisPoolConfig config = null;

    private Pool pool = null;


    private String poolType;

    private RedisConf conf;

    private String clientName;

    RedisJedisPool(RedisConf conf,String appName) {
        this.conf = conf;
        this.clientName = appName;
        initPool();
    }

    /**
     * 初始化bean 构建redis连接池 交给spring容器创建时即初始化连接池 加锁防止多线程不安全.
     */
    private synchronized Pool initPool() {
        poolType = conf.getType();
        if ("".equals(conf.getPassword())) {
            conf.setPassword(null);
        }
        if (pool == null) {
            log.info("init the redis the configure is {}", conf.getHost());
            if (conf.getType().equals("pool")) {
                String hosts = conf.getHost().get(0);
                String[] host = hosts.split(":");
                pool = new JedisPool(getPoolConf(conf),host[0], Integer.valueOf(host[1]),conf.getTimeout(),
                        conf.getPassword(),conf.getDbIndex(),clientName);
            } else if (conf.getType().equals("shared")) {
                throw new IllegalStateException("Redis 初始化失败！不支持该模式！");
            } else if (conf.getType().equals("master")) {

                Set<String> sentinels = new LinkedHashSet<String>();

                for (String hostStr : conf.getHost()) {
                    sentinels.add(hostStr);
                }
                pool = new JedisSentinelPool(conf.getMasterName(), sentinels, getPoolConf(conf),conf.getTimeout(),
                        conf.getPassword(),conf.getDbIndex(),clientName);
            }
        }
        log.info("init finished!", conf.getHost());
        return pool;
    }

    /**
     * 构建redis连接池配置
     */
    public JedisPoolConfig getPoolConf(RedisConf conf) {
        if (config == null) {
            config = new JedisPoolConfig();

            //控制一个pool可分配多少个jedis实例，通过pool.getResource()来获取；
            //如果赋值为-1，则表示不限制；如果pool已经分配了maxActive个jedis实例，则此时pool的状态为exhausted(耗尽)。
            if (conf.getMaxActive() > 0) {
                config.setMaxTotal(conf.getMaxActive());
            }
            //控制一个pool最多有多少个状态为idle(空闲的)的jedis实例。
            if (conf.getMaxIdle() > 0) {
                config.setMaxIdle(conf.getMaxIdle());
            }
            //表示当borrow(引入)一个jedis实例时，最大的等待时间，如果超过等待时间，则直接抛出JedisConnectionException；
            config.setMaxWaitMillis(conf.getMaxWaitMillis());
            //在borrow一个jedis实例时，是否提前进行validate操作；如果为true，则得到的jedis实例均是可用的；
            config.setTestOnBorrow(conf.getTestOnBorrow());
            //连接池内,最小空闲数,无论是否有使用,一直保持存活
            if (conf.getMinIdle() > 1){
                config.setMaxIdle(conf.getMinIdle());
            }
        }
        return config;
    }

    public Pool getPool() {
        if (pool == null) {
            initPool();
        }
        return pool;
    }

    @PreDestroy
    public void destroy() {
        try {
            log.info("RedisJedisPool destroy()关闭开始>>>>>>>>>>>>>>");
            if (this.pool != null) {
                this.pool.destroy();
            }
            log.info("RedisJedisPool destroy()关闭结束>>>>>>>>>>>>>>");
        } catch (Exception e) {
            log.error("关闭redis pool时发生异常.", e);
        }
    }

    public String getPoolType() {
        return poolType;
    }

}
