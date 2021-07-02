package site.wetsion.framework.infrastucture.cache;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.*;
import redis.clients.jedis.params.sortedset.ZAddParams;
import redis.clients.util.Slowlog;
import site.wetsion.framework.infrastucture.cache.callback.PiplineCallback;
import site.wetsion.framework.infrastucture.cache.callback.TransactionCallback;
import site.wetsion.framework.infrastucture.cache.util.SerializeUtil;

import java.io.UnsupportedEncodingException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * @author <a href="mailto:weixin@cai-inc.com">霜华</a>
 * @date 2021/6/30 5:43 PM
 **/
public class JedisClientUtil {

    private static final Logger log = LoggerFactory.getLogger(JedisClientUtil.class);

    private static final String MASTER = "master";
    private static final String CLUSTER = "cluster";

    private String scriptKey = "scriptKey";

    private RedisJedisPool redisJedisPool;


    JedisClientUtil(RedisJedisPool jedisPool) {
        this.redisJedisPool = jedisPool;
    }

    JedisClientUtil(JedisCluster jedisCluster, RedisJedisPool jedisPool) {
        this.jedisCluster = jedisCluster;
        this.redisJedisPool = jedisPool;
    }

    private JedisCluster jedisCluster;


    public JedisCluster getJedisCluster() {
        return jedisCluster;
    }


    public Object getResource() {
        if (CLUSTER.equals(redisJedisPool.getPoolType())) {
            return jedisCluster;
        }
        return redisJedisPool.getPool().getResource();
    }

    public void returnResource(Object resource) {
        if (CLUSTER.equals(redisJedisPool.getPoolType())) {
            return;
        }

        //释放连接
        try {
            if (resource == null) {
                return;
            }
            ((Jedis) resource).close();
        } catch (Exception e) {
            log.error("[JedisClientUtil][returnResource]happened error!", e);
        }
    }

    public String getValue(String key) {
        String rtn;
        Object jedis = null;
        try {
            jedis = getResource();
            if (CLUSTER.equals(redisJedisPool.getPoolType())) {
                rtn = ((JedisCluster) jedis).get(key);
            } else {
                rtn = ((Jedis) jedis).get(key);
            }
            return rtn;
        } catch (Exception e) {
            throw e;
        } finally {
            returnResource(jedis);
        }
    }

    public <T> String setObject(String key, T obj) {
        byte[] valueBytes = SerializeUtil.serialize(obj);
        return set(key.getBytes(), valueBytes);
    }

    public <T> String setObjectEx(String key, int seconds, T obj) {
        byte[] valueBytes = SerializeUtil.serialize(obj);
        return setex(key.getBytes(), seconds, valueBytes);
    }

    public <T> T getObject(String key, Class<T> targetClass) {
        byte[] bytes = get(key.getBytes());
        if (bytes != null && bytes.length > 0) {
            return SerializeUtil.deserialize(bytes, targetClass);
        } else {
            return null;
        }
    }

    public <T> String setList(String key, List<T> objList) {
        byte[] valueBytes = SerializeUtil.serializeList(objList);
        return set(key.getBytes(), valueBytes);
    }

    public <T> List<T> getList(String key, Class<T> targetClass) {
        byte[] bytes = get(key.getBytes());
        if (bytes != null && bytes.length > 0) {
            return SerializeUtil.deserializeList(bytes, targetClass);
        } else {
            return null;
        }
    }

    public Transaction getTranscation() {
        Object jedis = getResource();
        if (CLUSTER.equals(redisJedisPool.getPoolType())) {
            //            return ((JedisCluster) jedis).multi();
            throw new IllegalStateException("not support");
        } else {
            return ((Jedis) jedis).multi();
        }
    }

    public void execTransaction(TransactionCallback callback) {
        Jedis jedis = getJedis();
        try {
            callback.callback(jedis.multi());
        } catch (Exception e) {
            throw e;
        } finally {
            returnResource(jedis);
        }
    }

    public Jedis getJedis() {
        Object jedis = getResource();
        if (CLUSTER.equals(redisJedisPool.getPoolType())) {
            return null;
        } else {
            return (Jedis) jedis;
        }
    }

    /**
     * 该方法禁用，会导致连接池泄露
     * @return
     */
    @Deprecated
    public Pipeline pipelined() {
        //        Object jedis = getResource();

        if (CLUSTER.equals(redisJedisPool.getPoolType())) {
            throw new UnsupportedOperationException("集群不支持该操作");
        } else {
            throw new UnsupportedOperationException("该方法会导致连接池泄露，具体请参考");
        }

    }



    public void execPipeline(PiplineCallback callback) {
        if (CLUSTER.equals(redisJedisPool.getPoolType())) {
            throw new IllegalStateException("集群版redis不支持pipelined");
        }

        Jedis jedis = getJedis();
        try {
            callback.callback(jedis.pipelined());
        } catch (Exception e) {
            throw e;
        } finally {
            returnResource(jedis);
        }
    }

    public Object eval(String script, int keyCount, String... params) {
        Object rtn;
        Object jedis = null;
        try {
            jedis = getResource();
            if (CLUSTER.equals(redisJedisPool.getPoolType())) {
                rtn = ((JedisCluster) jedis).eval(script, keyCount, params);
            } else {
                rtn = ((Jedis) jedis).eval(script, keyCount, params);
            }
            return rtn;
        } catch (Exception e) {
            throw e;
        } finally {
            returnResource(jedis);
        }
    }

    public Object eval(String script, List<String> keys, List<String> args) {
        Object rtn;
        Object jedis = null;
        try {
            jedis = getResource();
            if (CLUSTER.equals(redisJedisPool.getPoolType())) {
                rtn = ((JedisCluster) jedis).eval(script, keys, args);
            } else {
                rtn = ((Jedis) jedis).eval(script, keys, args);
            }
            return rtn;
        } catch (Exception e) {
            throw e;
        } finally {
            returnResource(jedis);
        }
    }

    public Object eval(String script) {
        Object rtn;
        Object jedis = null;
        try {
            jedis = getResource();
            if (CLUSTER.equals(redisJedisPool.getPoolType())) {
                rtn = ((JedisCluster) jedis).eval(script, scriptKey);
            } else {
                rtn = ((Jedis) jedis).eval(script);
            }
            return rtn;
        } catch (Exception e) {
            throw e;
        } finally {
            returnResource(jedis);
        }
    }

    public Object evalsha(String script) {
        Object rtn;
        Object jedis = null;
        try {
            jedis = getResource();
            if (CLUSTER.equals(redisJedisPool.getPoolType())) {
                rtn = ((JedisCluster) jedis).evalsha(script, scriptKey);
            } else {
                rtn = ((Jedis) jedis).evalsha(script);
            }
            return rtn;
        } catch (Exception e) {
            throw e;
        } finally {
            returnResource(jedis);
        }
    }

    public Object evalsha(String sha1, List<String> keys, List<String> args) {
        Object rtn;
        Object jedis = null;
        try {
            jedis = getResource();
            if (CLUSTER.equals(redisJedisPool.getPoolType())) {
                rtn = ((JedisCluster) jedis).evalsha(sha1, keys, args);
            } else {
                rtn = ((Jedis) jedis).evalsha(sha1, keys, args);
            }
            return rtn;
        } catch (Exception e) {
            throw e;
        } finally {
            returnResource(jedis);
        }
    }

    public Object evalsha(String sha1, int keyCount, String... params) {
        Object rtn;
        Object jedis = null;
        try {
            jedis = getResource();
            if (CLUSTER.equals(redisJedisPool.getPoolType())) {
                rtn = ((JedisCluster) jedis).evalsha(sha1, keyCount, params);
            } else {
                rtn = ((Jedis) jedis).evalsha(sha1, keyCount, params);
            }
            return rtn;
        } catch (Exception e) {
            throw e;
        } finally {
            returnResource(jedis);
        }
    }

    public Boolean scriptExists(String sha1) {
        Boolean rtn;
        Object jedis = null;
        try {
            jedis = getResource();
            if (CLUSTER.equals(redisJedisPool.getPoolType())) {
                rtn = ((JedisCluster) jedis).scriptExists(scriptKey, sha1);
            } else {
                rtn = ((Jedis) jedis).scriptExists(sha1);
            }
            return rtn;
        } catch (Exception e) {
            throw e;
        } finally {
            returnResource(jedis);
        }
    }

    public List<Boolean> scriptExists(String... sha1) {
        List<Boolean> rtn;
        Object jedis = null;
        try {
            jedis = getResource();
            if (CLUSTER.equals(redisJedisPool.getPoolType())) {
                rtn = ((JedisCluster) jedis).scriptExists(scriptKey, sha1);
            } else {
                rtn = ((Jedis) jedis).scriptExists(sha1);
            }
            return rtn;
        } catch (Exception e) {
            throw e;
        } finally {
            returnResource(jedis);
        }
    }

    public String scriptLoad(String script) {
        String rtn;
        Object jedis = null;
        try {
            jedis = getResource();
            if (CLUSTER.equals(redisJedisPool.getPoolType())) {
                rtn = ((JedisCluster) jedis).scriptLoad(script, scriptKey);
            } else {
                rtn = ((Jedis) jedis).scriptLoad(script);
            }
            return rtn;
        } catch (Exception e) {
            throw e;
        } finally {
            returnResource(jedis);
        }
    }

    public String scriptLoad(String script, String key) {
        String rtn;
        Object jedis = null;
        try {
            jedis = getResource();
            if (CLUSTER.equals(redisJedisPool.getPoolType())) {
                rtn = ((JedisCluster) jedis).scriptLoad(script, key);
            } else {
                rtn = ((Jedis) jedis).scriptLoad(script);
            }
            return rtn;
        } catch (Exception e) {
            throw e;
        } finally {
            returnResource(jedis);
        }
    }

    public List<String> configGet(String pattern) {
        throw new IllegalStateException("not support");
    }

    public String configSet(String parameter, String value) {
        throw new IllegalStateException("not support");
    }

    public List<Slowlog> slowlogGet() {
        throw new IllegalStateException("not support");
    }

    public List<Slowlog> slowlogGet(long entries) {
        throw new IllegalStateException("not support");
    }

    public Long objectRefcount(String string) {
        Long rtn;
        Object jedis = null;
        try {
            jedis = getResource();
            if (CLUSTER.equals(redisJedisPool.getPoolType())) {
                //                rtn = ((JedisCluster)jedis).objectRefcount(string);'
                throw new IllegalStateException("not support");
            } else {
                rtn = ((Jedis) jedis).objectRefcount(string);
            }
            return rtn;
        } catch (Exception e) {
            throw e;
        } finally {
            returnResource(jedis);
        }
    }

    public String objectEncoding(String string) {
        String rtn;
        Object jedis = null;
        try {
            jedis = getResource();
            if (CLUSTER.equals(redisJedisPool.getPoolType())) {
                //                rtn = ((JedisCluster)jedis).objectEncoding(string);
                throw new IllegalStateException("not support");
            } else {
                rtn = ((Jedis) jedis).objectEncoding(string);
            }
            return rtn;
        } catch (Exception e) {
            throw e;
        } finally {
            returnResource(jedis);
        }
    }

    public Long objectIdletime(String string) {
        Long rtn;
        Object jedis = null;
        try {
            jedis = getResource();
            if (CLUSTER.equals(redisJedisPool.getPoolType())) {
                //                rtn = ((JedisCluster)jedis).objectIdletime(string);
                throw new IllegalStateException("not support");
            } else {
                rtn = ((Jedis) jedis).objectIdletime(string);
            }
            return rtn;
        } catch (Exception e) {
            throw e;
        } finally {
            returnResource(jedis);
        }
    }

    public Long del(String... keys) {
        Long rtn;
        Object jedis = null;
        try {
            jedis = getResource();
            if (CLUSTER.equals(redisJedisPool.getPoolType())) {
                rtn = ((JedisCluster) jedis).del(keys);
            } else {
                rtn = ((Jedis) jedis).del(keys);
            }
            return rtn;
        } catch (Exception e) {
            throw e;
        } finally {
            returnResource(jedis);
        }
    }

    public Long exists(String... keys) {
        Long rtn;
        Object jedis = null;
        try {
            jedis = getResource();
            if (CLUSTER.equals(redisJedisPool.getPoolType())) {
                rtn = ((JedisCluster) jedis).exists(keys);
            } else {
                rtn = ((Jedis) jedis).exists(keys);
            }
            return rtn;
        } catch (Exception e) {
            throw e;
        } finally {
            returnResource(jedis);
        }
    }

    public List<String> blpop(int timeout, String... keys) {
        List<String> rtn;
        Object jedis = null;
        try {
            jedis = getResource();
            if (CLUSTER.equals(redisJedisPool.getPoolType())) {
                rtn = ((JedisCluster) jedis).blpop(timeout, keys);
            } else {
                rtn = ((Jedis) jedis).blpop(timeout, keys);
            }
            return rtn;
        } catch (Exception e) {
            throw e;
        } finally {
            returnResource(jedis);
        }
    }

    public List<String> brpop(int timeout, String... keys) {
        List<String> rtn;
        Object jedis = null;
        try {
            jedis = getResource();
            if (CLUSTER.equals(redisJedisPool.getPoolType())) {
                rtn = ((JedisCluster) jedis).brpop(timeout, keys);
            } else {
                rtn = ((Jedis) jedis).brpop(timeout, keys);
            }
            return rtn;
        } catch (Exception e) {
            throw e;
        } finally {
            returnResource(jedis);
        }
    }

    public List<String> blpop(String... args) {
        List<String> rtn;
        Object jedis = null;
        try {
            jedis = getResource();
            if (CLUSTER.equals(redisJedisPool.getPoolType())) {
                rtn = ((JedisCluster) jedis).blpop(Integer.MAX_VALUE, args);
            } else {
                rtn = ((Jedis) jedis).blpop(args);
            }
            return rtn;
        } catch (Exception e) {
            throw e;
        } finally {
            returnResource(jedis);
        }
    }

    public List<String> brpop(String... args) {
        List<String> rtn;
        Object jedis = null;
        try {
            jedis = getResource();
            if (CLUSTER.equals(redisJedisPool.getPoolType())) {
                rtn = ((JedisCluster) jedis).brpop(Integer.MAX_VALUE, args);
            } else {
                rtn = ((Jedis) jedis).brpop(args);
            }
            return rtn;
        } catch (Exception e) {
            throw e;
        } finally {
            returnResource(jedis);
        }
    }

    public Set<String> keys(String pattern) {
        return null;
    }

    public List<String> mget(String... keys) {
        List<String> rtn;
        Object jedis = null;
        try {
            jedis = getResource();
            if (CLUSTER.equals(redisJedisPool.getPoolType())) {
                rtn = ((JedisCluster) jedis).mget(keys);
            } else {
                rtn = ((Jedis) jedis).mget(keys);
            }
            return rtn;
        } catch (Exception e) {
            throw e;
        } finally {
            returnResource(jedis);
        }
    }

    public String mset(String... keysvalues) {
        String rtn;
        Object jedis = null;
        try {
            jedis = getResource();
            if (CLUSTER.equals(redisJedisPool.getPoolType())) {
                rtn = ((JedisCluster) jedis).mset(keysvalues);
            } else {
                rtn = ((Jedis) jedis).mset(keysvalues);
            }
            return rtn;
        } catch (Exception e) {
            throw e;
        } finally {
            returnResource(jedis);
        }
    }

    public Long msetnx(String... keysvalues) {
        Long rtn;
        Object jedis = null;
        try {
            jedis = getResource();
            if (CLUSTER.equals(redisJedisPool.getPoolType())) {
                rtn = ((JedisCluster) jedis).msetnx(keysvalues);
            } else {
                rtn = ((Jedis) jedis).msetnx(keysvalues);
            }
            return rtn;
        } catch (Exception e) {
            throw e;
        } finally {
            returnResource(jedis);
        }
    }

    public String rename(String oldkey, String newkey) {
        String rtn;
        Object jedis = null;
        try {
            jedis = getResource();
            if (CLUSTER.equals(redisJedisPool.getPoolType())) {
                rtn = ((JedisCluster) jedis).rename(oldkey, newkey);
            } else {
                rtn = ((Jedis) jedis).rename(oldkey, newkey);
            }
            return rtn;
        } catch (Exception e) {
            throw e;
        } finally {
            returnResource(jedis);
        }
    }

    public Long renamenx(String oldkey, String newkey) {
        Long rtn;
        Object jedis = null;
        try {
            jedis = getResource();
            if (CLUSTER.equals(redisJedisPool.getPoolType())) {
                rtn = ((JedisCluster) jedis).renamenx(oldkey, newkey);
            } else {
                rtn = ((Jedis) jedis).renamenx(oldkey, newkey);
            }
            return rtn;
        } catch (Exception e) {
            throw e;
        } finally {
            returnResource(jedis);
        }
    }

    public String rpoplpush(String srckey, String dstkey) {
        String rtn;
        Object jedis = null;
        try {
            jedis = getResource();
            if (CLUSTER.equals(redisJedisPool.getPoolType())) {
                rtn = ((JedisCluster) jedis).rpoplpush(srckey, dstkey);
            } else {
                rtn = ((Jedis) jedis).rpoplpush(srckey, dstkey);
            }
            return rtn;
        } catch (Exception e) {
            throw e;
        } finally {
            returnResource(jedis);
        }
    }

    public Set<String> sdiff(String... keys) {
        Set<String> rtn;
        Object jedis = null;
        try {
            jedis = getResource();
            if (CLUSTER.equals(redisJedisPool.getPoolType())) {
                rtn = ((JedisCluster) jedis).sdiff(keys);
            } else {
                rtn = ((Jedis) jedis).sdiff(keys);
            }
            return rtn;
        } catch (Exception e) {
            throw e;
        } finally {
            returnResource(jedis);
        }
    }

    public Long sdiffstore(String dstkey, String... keys) {
        Long rtn;
        Object jedis = null;
        try {
            jedis = getResource();
            if (CLUSTER.equals(redisJedisPool.getPoolType())) {
                rtn = ((JedisCluster) jedis).sdiffstore(dstkey, keys);
            } else {
                rtn = ((Jedis) jedis).sdiffstore(dstkey, keys);
            }
            return rtn;
        } catch (Exception e) {
            throw e;
        } finally {
            returnResource(jedis);
        }
    }

    public Set<String> sinter(String... keys) {
        Set<String> rtn;
        Object jedis = null;
        try {
            jedis = getResource();
            if (CLUSTER.equals(redisJedisPool.getPoolType())) {
                rtn = ((JedisCluster) jedis).sinter(keys);
            } else {
                rtn = ((Jedis) jedis).sinter(keys);
            }
            return rtn;
        } catch (Exception e) {
            throw e;
        } finally {
            returnResource(jedis);
        }
    }

    public Long sinterstore(String dstkey, String... keys) {
        Long rtn;
        Object jedis = null;
        try {
            jedis = getResource();
            if (CLUSTER.equals(redisJedisPool.getPoolType())) {
                rtn = ((JedisCluster) jedis).sinterstore(dstkey, keys);
            } else {
                rtn = ((Jedis) jedis).sinterstore(dstkey, keys);
            }
            return rtn;
        } catch (Exception e) {
            throw e;
        } finally {
            returnResource(jedis);
        }
    }

    public Long smove(String srckey, String dstkey, String member) {
        Long rtn;
        Object jedis = null;
        try {
            jedis = getResource();
            if (CLUSTER.equals(redisJedisPool.getPoolType())) {
                rtn = ((JedisCluster) jedis).smove(srckey, dstkey, member);
            } else {
                rtn = ((Jedis) jedis).smove(srckey, dstkey, member);
            }
            return rtn;
        } catch (Exception e) {
            throw e;
        } finally {
            returnResource(jedis);
        }
    }

    public Long sort(String key, SortingParams sortingParameters, String dstkey) {
        Long rtn;
        Object jedis = null;
        try {
            jedis = getResource();
            if (CLUSTER.equals(redisJedisPool.getPoolType())) {
                rtn = ((JedisCluster) jedis).sort(key, sortingParameters, dstkey);
            } else {
                rtn = ((Jedis) jedis).sort(key, sortingParameters, dstkey);
            }
            return rtn;
        } catch (Exception e) {
            throw e;
        } finally {
            returnResource(jedis);
        }
    }

    public Long sort(String key, String dstkey) {
        Long rtn;
        Object jedis = null;
        try {
            jedis = getResource();
            if (CLUSTER.equals(redisJedisPool.getPoolType())) {
                rtn = ((JedisCluster) jedis).sort(key, dstkey);
            } else {
                rtn = ((Jedis) jedis).sort(key, dstkey);
            }
            return rtn;
        } catch (Exception e) {
            throw e;
        } finally {
            returnResource(jedis);
        }
    }

    public Set<String> sunion(String... keys) {
        Set<String> rtn;
        Object jedis = null;
        try {
            jedis = getResource();
            if (CLUSTER.equals(redisJedisPool.getPoolType())) {
                rtn = ((JedisCluster) jedis).sunion(keys);
            } else {
                rtn = ((Jedis) jedis).sunion(keys);
            }
            return rtn;
        } catch (Exception e) {
            throw e;
        } finally {
            returnResource(jedis);
        }
    }

    public Long sunionstore(String dstkey, String... keys) {
        Long rtn;
        Object jedis = null;
        try {
            jedis = getResource();
            if (CLUSTER.equals(redisJedisPool.getPoolType())) {
                rtn = ((JedisCluster) jedis).sunionstore(dstkey, keys);
            } else {
                rtn = ((Jedis) jedis).sunionstore(dstkey, keys);
            }
            return rtn;
        } catch (Exception e) {
            throw e;
        } finally {
            returnResource(jedis);
        }
    }

    public String watch(String... keys) {
        String rtn;
        Object jedis = null;
        try {
            jedis = getResource();
            if (CLUSTER.equals(redisJedisPool.getPoolType())) {
                //                rtn = ((JedisCluster)jedis).watch(keys);
                throw new IllegalStateException("not support");
            } else {
                rtn = ((Jedis) jedis).watch(keys);
            }
            return rtn;
        } catch (Exception e) {
            throw e;
        } finally {
            returnResource(jedis);
        }
    }

    public Long zinterstore(String dstkey, String... sets) {
        Long rtn;
        Object jedis = null;
        try {
            jedis = getResource();
            if (CLUSTER.equals(redisJedisPool.getPoolType())) {
                rtn = ((JedisCluster) jedis).zinterstore(dstkey, sets);
            } else {
                rtn = ((Jedis) jedis).zinterstore(dstkey, sets);
            }
            return rtn;
        } catch (Exception e) {
            throw e;
        } finally {
            returnResource(jedis);
        }
    }

    public Long zinterstore(String dstkey, ZParams params, String... sets) {
        Long rtn;
        Object jedis = null;
        try {
            jedis = getResource();
            if (CLUSTER.equals(redisJedisPool.getPoolType())) {
                rtn = ((JedisCluster) jedis).zinterstore(dstkey, params, sets);
            } else {
                rtn = ((Jedis) jedis).zinterstore(dstkey, params, sets);
            }
            return rtn;
        } catch (Exception e) {
            throw e;
        } finally {
            returnResource(jedis);
        }
    }

    public Long zunionstore(String dstkey, String... sets) {
        Long rtn;
        Object jedis = null;
        try {
            jedis = getResource();
            if (CLUSTER.equals(redisJedisPool.getPoolType())) {
                rtn = ((JedisCluster) jedis).zunionstore(dstkey, sets);
            } else {
                rtn = ((Jedis) jedis).zunionstore(dstkey, sets);
            }
            return rtn;
        } catch (Exception e) {
            throw e;
        } finally {
            returnResource(jedis);
        }
    }

    public Long zunionstore(String dstkey, ZParams params, String... sets) {
        Long rtn;
        Object jedis = null;
        try {
            jedis = getResource();
            if (CLUSTER.equals(redisJedisPool.getPoolType())) {
                rtn = ((JedisCluster) jedis).zunionstore(dstkey, params, sets);
            } else {
                rtn = ((Jedis) jedis).zunionstore(dstkey, params, sets);
            }
            return rtn;
        } catch (Exception e) {
            throw e;
        } finally {
            returnResource(jedis);
        }
    }

    public String brpoplpush(String source, String destination, int timeout) {
        String rtn;
        Object jedis = null;
        try {
            jedis = getResource();
            if (CLUSTER.equals(redisJedisPool.getPoolType())) {
                rtn = ((JedisCluster) jedis).brpoplpush(source, destination, timeout);
            } else {
                rtn = ((Jedis) jedis).brpoplpush(source, destination, timeout);
            }
            return rtn;
        } catch (Exception e) {
            throw e;
        } finally {
            returnResource(jedis);
        }
    }

    public Long publish(String channel, String message) {
        Long rtn;
        Object jedis = null;
        try {
            jedis = getResource();
            if (CLUSTER.equals(redisJedisPool.getPoolType())) {
                rtn = ((JedisCluster) jedis).publish(channel, message);
            } else {
                rtn = ((Jedis) jedis).publish(channel, message);
            }
            return rtn;
        } catch (Exception e) {
            throw e;
        } finally {
            returnResource(jedis);
        }
    }

    public void subscribe(JedisPubSub jedisPubSub, String... channels) {
        Object jedis = null;
        try {
            jedis = getResource();
            if (CLUSTER.equals(redisJedisPool.getPoolType())) {
                ((JedisCluster) jedis).subscribe(jedisPubSub, channels);
            } else {
                ((Jedis) jedis).subscribe(jedisPubSub, channels);
            }
        } catch (Exception e) {
            throw e;
        } finally {
            returnResource(jedis);
        }
    }

    public void psubscribe(JedisPubSub jedisPubSub, String... patterns) {
        Object jedis = null;
        try {
            jedis = getResource();
            if (CLUSTER.equals(redisJedisPool.getPoolType())) {
                ((JedisCluster) jedis).psubscribe(jedisPubSub, patterns);
            } else {
                ((Jedis) jedis).psubscribe(jedisPubSub, patterns);
            }
        } catch (Exception e) {
            throw e;
        } finally {
            returnResource(jedis);
        }
    }

    public String randomKey() {
        String rtn;
        Object jedis = null;
        try {
            jedis = getResource();
            if (CLUSTER.equals(redisJedisPool.getPoolType())) {
                //                rtn = ((JedisCluster)jedis).randomKey();
                throw new IllegalStateException("not support");
            } else {
                rtn = ((Jedis) jedis).randomKey();
            }
            return rtn;
        } catch (Exception e) {
            throw e;
        } finally {
            returnResource(jedis);
        }
    }

    public Long bitop(BitOP op, String destKey, String... srcKeys) {
        Long rtn;
        Object jedis = null;
        try {
            jedis = getResource();
            if (CLUSTER.equals(redisJedisPool.getPoolType())) {
                rtn = ((JedisCluster) jedis).bitop(op, destKey, srcKeys);
            } else {
                rtn = ((Jedis) jedis).bitop(op, destKey, srcKeys);
            }
            return rtn;
        } catch (Exception e) {
            throw e;
        } finally {
            returnResource(jedis);
        }
    }

    public ScanResult<String> scan(int cursor) {
        return null;
    }

    public ScanResult<String> scan(String cursor) {
        ScanResult<String> rtn;
        Object jedis = null;
        try {
            jedis = getResource();
            if (CLUSTER.equals(redisJedisPool.getPoolType())) {
                ScanParams scanParams = new ScanParams();
                rtn = ((JedisCluster) jedis).scan(cursor, scanParams);

            } else {
                rtn = ((Jedis) jedis).scan(cursor);
            }
            return rtn;
        } catch (Exception e) {
            throw e;
        } finally {
            returnResource(jedis);
        }
    }

    public ScanResult<String> scan(String key, ScanParams scanParams) {
        ScanResult<String> rtn;
        Object jedis = null;
        try {
            jedis = getResource();
            if (CLUSTER.equals(redisJedisPool.getPoolType())) {
                rtn = ((JedisCluster) jedis).scan(key, scanParams);
            } else {
                rtn = ((Jedis) jedis).scan(key, scanParams);
            }
            return rtn;
        } catch (Exception e) {
            throw e;
        } finally {
            returnResource(jedis);
        }
    }

    public String pfmerge(String destkey, String... sourcekeys) {
        String rtn;
        Object jedis = null;
        try {
            jedis = getResource();
            if (CLUSTER.equals(redisJedisPool.getPoolType())) {
                rtn = ((JedisCluster) jedis).pfmerge(destkey, sourcekeys);
            } else {
                rtn = ((Jedis) jedis).pfmerge(destkey, sourcekeys);
            }
            return rtn;
        } catch (Exception e) {
            throw e;
        } finally {
            returnResource(jedis);
        }
    }

    public long pfcount(String... keys) {
        Long rtn;
        Object jedis = null;
        try {
            jedis = getResource();
            if (CLUSTER.equals(redisJedisPool.getPoolType())) {
                rtn = ((JedisCluster) jedis).pfcount(keys);
            } else {
                rtn = ((Jedis) jedis).pfcount(keys);
            }
            return rtn;
        } catch (Exception e) {
            throw e;
        } finally {
            returnResource(jedis);
        }
    }

    public String set(String key, String value) {
        String rtn;
        Object jedis = null;
        try {
            jedis = getResource();
            if (CLUSTER.equals(redisJedisPool.getPoolType())) {
                rtn = ((JedisCluster) jedis).set(key, value);
            } else {
                rtn = ((Jedis) jedis).set(key, value);
            }
            return rtn;
        } catch (Exception e) {
            throw e;
        } finally {
            returnResource(jedis);
        }
    }

    public String set(String key, String value, SetPremise nxxx) {
        String rtn;
        Object jedis = null;
        try {
            jedis = getResource();
            if (CLUSTER.equals(redisJedisPool.getPoolType())) {
                rtn = ((JedisCluster) jedis).set(key, value, nxxx.value);
            } else {
                rtn = ((Jedis) jedis).set(key, value, nxxx.value);
            }
            return rtn;
        } catch (Exception e) {
            throw e;
        } finally {
            returnResource(jedis);
        }
    }

    public String set(String key, String value, SetPremise nxxx, ExpireType expx, long time) {
        String rtn;
        Object jedis = null;
        try {
            jedis = getResource();
            if (CLUSTER.equals(redisJedisPool.getPoolType())) {
                rtn = ((JedisCluster) jedis).set(key, value, nxxx.value, expx.value, time);
            } else {
                rtn = ((Jedis) jedis).set(key, value, nxxx.value, expx.value, time);
            }
            return rtn;
        } catch (Exception e) {
            throw e;
        } finally {
            returnResource(jedis);
        }
    }

    public String get(String key) {
        String rtn;
        Object jedis = null;
        try {
            jedis = getResource();
            if (CLUSTER.equals(redisJedisPool.getPoolType())) {
                rtn = ((JedisCluster) jedis).get(key);
            } else {
                rtn = ((Jedis) jedis).get(key);
            }
            return rtn;
        } catch (Exception e) {
            throw e;
        } finally {
            returnResource(jedis);
        }
    }

    public Boolean exists(String key) {
        Boolean rtn;
        Object jedis = null;
        try {
            jedis = getResource();
            if (CLUSTER.equals(redisJedisPool.getPoolType())) {
                rtn = ((JedisCluster) jedis).exists(key);
            } else {
                rtn = ((Jedis) jedis).exists(key);
            }
            return rtn;
        } catch (Exception e) {
            throw e;
        } finally {
            returnResource(jedis);
        }
    }

    public Long persist(String key) {
        Long rtn;
        Object jedis = null;
        try {
            jedis = getResource();
            if (CLUSTER.equals(redisJedisPool.getPoolType())) {
                rtn = ((JedisCluster) jedis).persist(key);
            } else {
                rtn = ((Jedis) jedis).persist(key);
            }
            return rtn;
        } catch (Exception e) {
            throw e;
        } finally {
            returnResource(jedis);
        }
    }

    public String type(String key) {
        String rtn;
        Object jedis = null;
        try {
            jedis = getResource();
            if (CLUSTER.equals(redisJedisPool.getPoolType())) {
                rtn = ((JedisCluster) jedis).type(key);
            } else {
                rtn = ((Jedis) jedis).type(key);
            }
            return rtn;
        } catch (Exception e) {
            throw e;
        } finally {
            returnResource(jedis);
        }
    }

    public Long expire(String key, int seconds) {
        Long rtn;
        Object jedis = null;
        try {
            jedis = getResource();
            if (CLUSTER.equals(redisJedisPool.getPoolType())) {
                rtn = ((JedisCluster) jedis).expire(key, seconds);
            } else {
                rtn = ((Jedis) jedis).expire(key, seconds);
            }
            return rtn;
        } catch (Exception e) {
            throw e;
        } finally {
            returnResource(jedis);
        }
    }

    public Long expireAt(String key, long unixTime) {
        Long rtn;
        Object jedis = null;
        try {
            jedis = getResource();
            if (CLUSTER.equals(redisJedisPool.getPoolType())) {
                rtn = ((JedisCluster) jedis).expireAt(key, unixTime);
            } else {
                rtn = ((Jedis) jedis).expireAt(key, unixTime);
            }
            return rtn;
        } catch (Exception e) {
            throw e;
        } finally {
            returnResource(jedis);
        }
    }

    public Long pexpireAt(String key, long l) {
        Long rtn;
        Object jedis = null;
        try {
            jedis = getResource();
            if (CLUSTER.equals(redisJedisPool.getPoolType())) {
                rtn = ((JedisCluster) jedis).pexpireAt(key, l);
            } else {
                rtn = ((Jedis) jedis).pexpireAt(key, l);
            }
            return rtn;
        } catch (Exception e) {
            throw e;
        } finally {
            returnResource(jedis);
        }
    }

    public Long ttl(String key) {
        Long rtn;
        Object jedis = null;
        try {
            jedis = getResource();
            if (CLUSTER.equals(redisJedisPool.getPoolType())) {
                rtn = ((JedisCluster) jedis).ttl(key);
            } else {
                rtn = ((Jedis) jedis).ttl(key);
            }
            return rtn;
        } catch (Exception e) {
            throw e;
        } finally {
            returnResource(jedis);
        }
    }

    public Long pttl(String s) {
        Long rtn;
        Object jedis = null;
        try {
            jedis = getResource();
            if (CLUSTER.equals(redisJedisPool.getPoolType())) {
                rtn = ((JedisCluster) jedis).pttl(s);
            } else {
                rtn = ((Jedis) jedis).pttl(s);
            }
            return rtn;
        } catch (Exception e) {
            throw e;
        } finally {
            returnResource(jedis);
        }
    }

    public Boolean setbit(String key, long offset, boolean value) {
        Boolean rtn;
        Object jedis = null;
        try {
            jedis = getResource();
            if (CLUSTER.equals(redisJedisPool.getPoolType())) {
                rtn = ((JedisCluster) jedis).setbit(key, offset, value);
            } else {
                rtn = ((Jedis) jedis).setbit(key, offset, value);
            }
            return rtn;
        } catch (Exception e) {
            throw e;
        } finally {
            returnResource(jedis);
        }
    }

    public Boolean setbit(String key, long offset, String value) {
        Boolean rtn;
        Object jedis = null;
        try {
            jedis = getResource();
            if (CLUSTER.equals(redisJedisPool.getPoolType())) {
                rtn = ((JedisCluster) jedis).setbit(key, offset, value);
            } else {
                rtn = ((Jedis) jedis).setbit(key, offset, value);
            }
            return rtn;
        } catch (Exception e) {
            throw e;
        } finally {
            returnResource(jedis);
        }
    }

    public Boolean getbit(String key, long offset) {
        Boolean rtn;
        Object jedis = null;
        try {
            jedis = getResource();
            if (CLUSTER.equals(redisJedisPool.getPoolType())) {
                rtn = ((JedisCluster) jedis).getbit(key, offset);
            } else {
                rtn = ((Jedis) jedis).getbit(key, offset);
            }
            return rtn;
        } catch (Exception e) {
            throw e;
        } finally {
            returnResource(jedis);
        }
    }

    public Long setrange(String key, long offset, String value) {
        Long rtn;
        Object jedis = null;
        try {
            jedis = getResource();
            if (CLUSTER.equals(redisJedisPool.getPoolType())) {
                rtn = ((JedisCluster) jedis).setrange(key, offset, value);
            } else {
                rtn = ((Jedis) jedis).setrange(key, offset, value);
            }
            return rtn;
        } catch (Exception e) {
            throw e;
        } finally {
            returnResource(jedis);
        }
    }

    public String getrange(String key, long startOffset, long endOffset) {
        String rtn;
        Object jedis = null;
        try {
            jedis = getResource();
            if (CLUSTER.equals(redisJedisPool.getPoolType())) {
                rtn = ((JedisCluster) jedis).getrange(key, startOffset, endOffset);
            } else {
                rtn = ((Jedis) jedis).getrange(key, startOffset, endOffset);
            }
            return rtn;
        } catch (Exception e) {
            throw e;
        } finally {
            returnResource(jedis);
        }
    }

    public String getSet(String key, String value) {
        String rtn;
        Object jedis = null;
        try {
            jedis = getResource();
            if (CLUSTER.equals(redisJedisPool.getPoolType())) {
                rtn = ((JedisCluster) jedis).getSet(key, value);
            } else {
                rtn = ((Jedis) jedis).getSet(key, value);
            }
            return rtn;
        } catch (Exception e) {
            throw e;
        } finally {
            returnResource(jedis);
        }
    }

    public Long setnx(String key, String value) {
        Long rtn;
        Object jedis = null;
        try {
            jedis = getResource();
            if (CLUSTER.equals(redisJedisPool.getPoolType())) {
                rtn = ((JedisCluster) jedis).setnx(key, value);
            } else {
                rtn = ((Jedis) jedis).setnx(key, value);
            }
            return rtn;
        } catch (Exception e) {
            throw e;
        } finally {
            returnResource(jedis);
        }
    }

    public String setex(String key, int seconds, String value) {
        String rtn;
        Object jedis = null;
        try {
            jedis = getResource();
            if (CLUSTER.equals(redisJedisPool.getPoolType())) {
                rtn = ((JedisCluster) jedis).setex(key, seconds, value);
            } else {
                rtn = ((Jedis) jedis).setex(key, seconds, value);
            }
            return rtn;
        } catch (Exception e) {
            throw e;
        } finally {
            returnResource(jedis);
        }
    }

    public String psetex(String key, long milliseconds, String value) {
        String rtn;
        Object jedis = null;
        try {
            jedis = getResource();
            if (CLUSTER.equals(redisJedisPool.getPoolType())) {
                rtn = ((JedisCluster) jedis).psetex(key, milliseconds, value);
            } else {
                rtn = ((Jedis) jedis).psetex(key, milliseconds, value);
            }
            return rtn;
        } catch (Exception e) {
            throw e;
        } finally {
            returnResource(jedis);
        }
    }

    public Long decrBy(String key, long integer) {
        Long rtn;
        Object jedis = null;
        try {
            jedis = getResource();
            if (CLUSTER.equals(redisJedisPool.getPoolType())) {
                rtn = ((JedisCluster) jedis).decrBy(key, integer);
            } else {
                rtn = ((Jedis) jedis).decrBy(key, integer);
            }
            return rtn;
        } catch (Exception e) {
            throw e;
        } finally {
            returnResource(jedis);
        }
    }

    public Long decr(String key) {
        Long rtn;
        Object jedis = null;
        try {
            jedis = getResource();
            if (CLUSTER.equals(redisJedisPool.getPoolType())) {
                rtn = ((JedisCluster) jedis).decr(key);
            } else {
                rtn = ((Jedis) jedis).decr(key);
            }
            return rtn;
        } catch (Exception e) {
            throw e;
        } finally {
            returnResource(jedis);
        }
    }

    public Long incrBy(String key, long integer) {
        Long rtn;
        Object jedis = null;
        try {
            jedis = getResource();
            if (CLUSTER.equals(redisJedisPool.getPoolType())) {
                rtn = ((JedisCluster) jedis).incrBy(key, integer);
            } else {
                rtn = ((Jedis) jedis).incrBy(key, integer);
            }
            return rtn;
        } catch (Exception e) {
            throw e;
        } finally {
            returnResource(jedis);
        }
    }

    public Double incrByFloat(String key, double integer) {
        Double rtn;
        Object jedis = null;
        try {
            jedis = getResource();
            if (CLUSTER.equals(redisJedisPool.getPoolType())) {
                rtn = ((JedisCluster) jedis).incrByFloat(key, integer);
            } else {
                rtn = ((Jedis) jedis).incrByFloat(key, integer);
            }
            return rtn;
        } catch (Exception e) {
            throw e;
        } finally {
            returnResource(jedis);
        }
    }

    public Long incr(String key) {
        Long rtn;
        Object jedis = null;
        try {
            jedis = getResource();
            if (CLUSTER.equals(redisJedisPool.getPoolType())) {
                rtn = ((JedisCluster) jedis).incr(key);
            } else {
                rtn = ((Jedis) jedis).incr(key);
            }
            return rtn;
        } catch (Exception e) {
            throw e;
        } finally {
            returnResource(jedis);
        }
    }

    public Long append(String key, String value) {
        Long rtn;
        Object jedis = null;
        try {
            jedis = getResource();
            if (CLUSTER.equals(redisJedisPool.getPoolType())) {
                rtn = ((JedisCluster) jedis).append(key, value);
            } else {
                rtn = ((Jedis) jedis).append(key, value);
            }
            return rtn;
        } catch (Exception e) {
            throw e;
        } finally {
            returnResource(jedis);
        }
    }

    public String substr(String key, int start, int end) {
        String rtn;
        Object jedis = null;
        try {
            jedis = getResource();
            if (CLUSTER.equals(redisJedisPool.getPoolType())) {
                rtn = ((JedisCluster) jedis).substr(key, start, end);
            } else {
                rtn = ((Jedis) jedis).substr(key, start, end);
            }
            return rtn;
        } catch (Exception e) {
            throw e;
        } finally {
            returnResource(jedis);
        }
    }

    public Long hset(String key, String field, String value) {
        Long rtn;
        Object jedis = null;
        try {
            jedis = getResource();
            if (CLUSTER.equals(redisJedisPool.getPoolType())) {
                rtn = ((JedisCluster) jedis).hset(key, field, value);
            } else {
                rtn = ((Jedis) jedis).hset(key, field, value);
            }
            return rtn;
        } catch (Exception e) {
            throw e;
        } finally {
            returnResource(jedis);
        }
    }

    public String hget(String key, String field) {
        String rtn;
        Object jedis = null;
        try {
            jedis = getResource();
            if (CLUSTER.equals(redisJedisPool.getPoolType())) {
                rtn = ((JedisCluster) jedis).hget(key, field);
            } else {
                rtn = ((Jedis) jedis).hget(key, field);
            }
            return rtn;
        } catch (Exception e) {
            throw e;
        } finally {
            returnResource(jedis);
        }
    }

    public Long hsetnx(String key, String field, String value) {
        Long rtn;
        Object jedis = null;
        try {
            jedis = getResource();
            if (CLUSTER.equals(redisJedisPool.getPoolType())) {
                rtn = ((JedisCluster) jedis).hsetnx(key, field, value);
            } else {
                rtn = ((Jedis) jedis).hsetnx(key, field, value);
            }
            return rtn;
        } catch (Exception e) {
            throw e;
        } finally {
            returnResource(jedis);
        }
    }

    public String hmset(String key, Map<String, String> hash) {
        String rtn;
        Object jedis = null;
        try {
            jedis = getResource();
            if (CLUSTER.equals(redisJedisPool.getPoolType())) {
                rtn = ((JedisCluster) jedis).hmset(key, hash);
            } else {
                rtn = ((Jedis) jedis).hmset(key, hash);
            }
            return rtn;
        } catch (Exception e) {
            throw e;
        } finally {
            returnResource(jedis);
        }
    }

    public List<String> hmget(String key, String... fields) {
        List<String> rtn;
        Object jedis = null;
        try {
            jedis = getResource();
            if (CLUSTER.equals(redisJedisPool.getPoolType())) {
                rtn = ((JedisCluster) jedis).hmget(key, fields);
            } else {
                rtn = ((Jedis) jedis).hmget(key, fields);
            }
            return rtn;
        } catch (Exception e) {
            throw e;
        } finally {
            returnResource(jedis);
        }
    }

    public Long hincrBy(String key, String field, long value) {
        Long rtn;
        Object jedis = null;
        try {
            jedis = getResource();
            if (CLUSTER.equals(redisJedisPool.getPoolType())) {
                rtn = ((JedisCluster) jedis).hincrBy(key, field, value);
            } else {
                rtn = ((Jedis) jedis).hincrBy(key, field, value);
            }
            return rtn;
        } catch (Exception e) {
            throw e;
        } finally {
            returnResource(jedis);
        }
    }

    public Double hincrByFloat(String key, String field, double value) throws UnsupportedEncodingException {
        Double rtn;
        Object jedis = null;
        try {
            jedis = getResource();
            if (CLUSTER.equals(redisJedisPool.getPoolType())) {
                rtn = ((JedisCluster) jedis).hincrByFloat(key.getBytes("utf-8"), field.getBytes("utf-8"), value);
            } else {
                rtn = ((Jedis) jedis).hincrByFloat(key, field, value);
            }
            return rtn;
        } catch (Exception e) {
            throw e;
        } finally {
            returnResource(jedis);
        }
    }

    public Boolean hexists(String key, String field) {
        Boolean rtn;
        Object jedis = null;
        try {
            jedis = getResource();
            if (CLUSTER.equals(redisJedisPool.getPoolType())) {
                rtn = ((JedisCluster) jedis).hexists(key, field);
            } else {
                rtn = ((Jedis) jedis).hexists(key, field);
            }
            return rtn;
        } catch (Exception e) {
            throw e;
        } finally {
            returnResource(jedis);
        }
    }

    public Long hdel(String key, String... fields) {
        Long rtn;
        Object jedis = null;
        try {
            jedis = getResource();
            if (CLUSTER.equals(redisJedisPool.getPoolType())) {
                rtn = ((JedisCluster) jedis).hdel(key, fields);
            } else {
                rtn = ((Jedis) jedis).hdel(key, fields);
            }
            return rtn;
        } catch (Exception e) {
            throw e;
        } finally {
            returnResource(jedis);
        }
    }

    public Long hlen(String key) {
        Long rtn;
        Object jedis = null;
        try {
            jedis = getResource();
            if (CLUSTER.equals(redisJedisPool.getPoolType())) {
                rtn = ((JedisCluster) jedis).hlen(key);
            } else {
                rtn = ((Jedis) jedis).hlen(key);
            }
            return rtn;
        } catch (Exception e) {
            throw e;
        } finally {
            returnResource(jedis);
        }
    }

    public Set<String> hkeys(String key) {
        Set<String> rtn;
        Object jedis = null;
        try {
            jedis = getResource();
            if (CLUSTER.equals(redisJedisPool.getPoolType())) {
                rtn = ((JedisCluster) jedis).hkeys(key);
            } else {
                rtn = ((Jedis) jedis).hkeys(key);
            }
            return rtn;
        } catch (Exception e) {
            throw e;
        } finally {
            returnResource(jedis);
        }
    }

    public List<String> hvals(String key) {
        List<String> rtn;
        Object jedis = null;
        try {
            jedis = getResource();
            if (CLUSTER.equals(redisJedisPool.getPoolType())) {
                rtn = ((JedisCluster) jedis).hvals(key);
            } else {
                rtn = ((Jedis) jedis).hvals(key);
            }
            return rtn;
        } catch (Exception e) {
            throw e;
        } finally {
            returnResource(jedis);
        }
    }

    public Map<String, String> hgetAll(String key) {
        Map<String, String> rtn;
        Object jedis = null;
        try {
            jedis = getResource();
            if (CLUSTER.equals(redisJedisPool.getPoolType())) {
                rtn = ((JedisCluster) jedis).hgetAll(key);
            } else {
                rtn = ((Jedis) jedis).hgetAll(key);
            }
            return rtn;
        } catch (Exception e) {
            throw e;
        } finally {
            returnResource(jedis);
        }
    }

    public Long rpush(String key, String... strings) {
        Long rtn;
        Object jedis = null;
        try {
            jedis = getResource();
            if (CLUSTER.equals(redisJedisPool.getPoolType())) {
                rtn = ((JedisCluster) jedis).rpush(key, strings);
            } else {
                rtn = ((Jedis) jedis).rpush(key, strings);
            }
            return rtn;
        } catch (Exception e) {
            throw e;
        } finally {
            returnResource(jedis);
        }
    }

    public Long lpush(String key, String... strings) {
        Long rtn;
        Object jedis = null;
        try {
            jedis = getResource();
            if (CLUSTER.equals(redisJedisPool.getPoolType())) {
                rtn = ((JedisCluster) jedis).lpush(key, strings);
            } else {
                rtn = ((Jedis) jedis).lpush(key, strings);
            }
            return rtn;
        } catch (Exception e) {
            throw e;
        } finally {
            returnResource(jedis);
        }
    }

    public Long llen(String key) {
        Long rtn;
        Object jedis = null;
        try {
            jedis = getResource();
            if (CLUSTER.equals(redisJedisPool.getPoolType())) {
                rtn = ((JedisCluster) jedis).llen(key);
            } else {
                rtn = ((Jedis) jedis).llen(key);
            }
            return rtn;
        } catch (Exception e) {
            throw e;
        } finally {
            returnResource(jedis);
        }
    }

    public List<String> lrange(String key, long start, long end) {
        List<String> rtn;
        Object jedis = null;
        try {
            jedis = getResource();
            if (CLUSTER.equals(redisJedisPool.getPoolType())) {
                rtn = ((JedisCluster) jedis).lrange(key, start, end);
            } else {
                rtn = ((Jedis) jedis).lrange(key, start, end);
            }
            return rtn;
        } catch (Exception e) {
            throw e;
        } finally {
            returnResource(jedis);
        }
    }

    public String ltrim(String key, long start, long end) {
        String rtn;
        Object jedis = null;
        try {
            jedis = getResource();
            if (CLUSTER.equals(redisJedisPool.getPoolType())) {
                rtn = ((JedisCluster) jedis).ltrim(key, start, end);
            } else {
                rtn = ((Jedis) jedis).ltrim(key, start, end);
            }
            return rtn;
        } catch (Exception e) {
            throw e;
        } finally {
            returnResource(jedis);
        }
    }

    public String lindex(String key, long index) {
        String rtn;
        Object jedis = null;
        try {
            jedis = getResource();
            if (CLUSTER.equals(redisJedisPool.getPoolType())) {
                rtn = ((JedisCluster) jedis).lindex(key, index);
            } else {
                rtn = ((Jedis) jedis).lindex(key, index);
            }
            return rtn;
        } catch (Exception e) {
            throw e;
        } finally {
            returnResource(jedis);
        }
    }

    public String lset(String key, long index, String value) {
        String rtn;
        Object jedis = null;
        try {
            jedis = getResource();
            if (CLUSTER.equals(redisJedisPool.getPoolType())) {
                rtn = ((JedisCluster) jedis).lset(key, index, value);
            } else {
                rtn = ((Jedis) jedis).lset(key, index, value);
            }
            return rtn;
        } catch (Exception e) {
            throw e;
        } finally {
            returnResource(jedis);
        }
    }

    public Long lrem(String key, long count, String value) {
        Long rtn;
        Object jedis = null;
        try {
            jedis = getResource();
            if (CLUSTER.equals(redisJedisPool.getPoolType())) {
                rtn = ((JedisCluster) jedis).lrem(key, count, value);
            } else {
                rtn = ((Jedis) jedis).lrem(key, count, value);
            }
            return rtn;
        } catch (Exception e) {
            throw e;
        } finally {
            returnResource(jedis);
        }
    }

    public String lpop(String key) {
        String rtn;
        Object jedis = null;
        try {
            jedis = getResource();
            if (CLUSTER.equals(redisJedisPool.getPoolType())) {
                rtn = ((JedisCluster) jedis).lpop(key);
            } else {
                rtn = ((Jedis) jedis).lpop(key);
            }
            return rtn;
        } catch (Exception e) {
            throw e;
        } finally {
            returnResource(jedis);
        }
    }

    public String rpop(String key) {
        String rtn;
        Object jedis = null;
        try {
            jedis = getResource();
            if (CLUSTER.equals(redisJedisPool.getPoolType())) {
                rtn = ((JedisCluster) jedis).rpop(key);
            } else {
                rtn = ((Jedis) jedis).rpop(key);
            }
            return rtn;
        } catch (Exception e) {
            throw e;
        } finally {
            returnResource(jedis);
        }
    }

    public Long sadd(String key, String... members) {
        Long rtn;
        Object jedis = null;
        try {
            jedis = getResource();
            if (CLUSTER.equals(redisJedisPool.getPoolType())) {
                rtn = ((JedisCluster) jedis).sadd(key, members);
            } else {
                rtn = ((Jedis) jedis).sadd(key, members);
            }
            return rtn;
        } catch (Exception e) {
            throw e;
        } finally {
            returnResource(jedis);
        }
    }

    public Set<String> smembers(String key) {
        Set<String> rtn;
        Object jedis = null;
        try {
            jedis = getResource();
            if (CLUSTER.equals(redisJedisPool.getPoolType())) {
                rtn = ((JedisCluster) jedis).smembers(key);
            } else {
                rtn = ((Jedis) jedis).smembers(key);
            }
            return rtn;
        } catch (Exception e) {
            throw e;
        } finally {
            returnResource(jedis);
        }
    }

    public Long srem(String key, String... members) {
        Long rtn;
        Object jedis = null;
        try {
            jedis = getResource();
            if (CLUSTER.equals(redisJedisPool.getPoolType())) {
                rtn = ((JedisCluster) jedis).srem(key, members);
            } else {
                rtn = ((Jedis) jedis).srem(key, members);
            }
            return rtn;
        } catch (Exception e) {
            throw e;
        } finally {
            returnResource(jedis);
        }
    }

    public String spop(String key) {
        String rtn;
        Object jedis = null;
        try {
            jedis = getResource();
            if (CLUSTER.equals(redisJedisPool.getPoolType())) {
                rtn = ((JedisCluster) jedis).spop(key);
            } else {
                rtn = ((Jedis) jedis).spop(key);
            }
            return rtn;
        } catch (Exception e) {
            throw e;
        } finally {
            returnResource(jedis);
        }
    }

    public Set<String> spop(String key, long l) {
        Set<String> rtn;
        Object jedis = null;
        try {
            jedis = getResource();
            if (CLUSTER.equals(redisJedisPool.getPoolType())) {
                rtn = ((JedisCluster) jedis).spop(key, l);
            } else {
                rtn = ((Jedis) jedis).spop(key, l);
            }
            return rtn;
        } catch (Exception e) {
            throw e;
        } finally {
            returnResource(jedis);
        }
    }

    public Long scard(String key) {
        Long rtn;
        Object jedis = null;
        try {
            jedis = getResource();
            if (CLUSTER.equals(redisJedisPool.getPoolType())) {
                rtn = ((JedisCluster) jedis).scard(key);
            } else {
                rtn = ((Jedis) jedis).scard(key);
            }
            return rtn;
        } catch (Exception e) {
            throw e;
        } finally {
            returnResource(jedis);
        }
    }

    public Boolean sismember(String key, String member) {
        Boolean rtn;
        Object jedis = null;
        try {
            jedis = getResource();
            if (CLUSTER.equals(redisJedisPool.getPoolType())) {
                rtn = ((JedisCluster) jedis).sismember(key, member);
            } else {
                rtn = ((Jedis) jedis).sismember(key, member);
            }
            return rtn;
        } catch (Exception e) {
            throw e;
        } finally {
            returnResource(jedis);
        }
    }

    public String srandmember(String key) {
        String rtn;
        Object jedis = null;
        try {
            jedis = getResource();
            if (CLUSTER.equals(redisJedisPool.getPoolType())) {
                rtn = ((JedisCluster) jedis).srandmember(key);
            } else {
                rtn = ((Jedis) jedis).srandmember(key);
            }
            return rtn;
        } catch (Exception e) {
            throw e;
        } finally {
            returnResource(jedis);
        }
    }

    public List<String> srandmember(String key, int count) {
        List<String> rtn;
        Object jedis = null;
        try {
            jedis = getResource();
            if (CLUSTER.equals(redisJedisPool.getPoolType())) {
                rtn = ((JedisCluster) jedis).srandmember(key, count);
            } else {
                rtn = ((Jedis) jedis).srandmember(key, count);
            }
            return rtn;
        } catch (Exception e) {
            throw e;
        } finally {
            returnResource(jedis);
        }
    }

    public Long strlen(String key) {
        Long rtn;
        Object jedis = null;
        try {
            jedis = getResource();
            if (CLUSTER.equals(redisJedisPool.getPoolType())) {
                rtn = ((JedisCluster) jedis).strlen(key);
            } else {
                rtn = ((Jedis) jedis).strlen(key);
            }
            return rtn;
        } catch (Exception e) {
            throw e;
        } finally {
            returnResource(jedis);
        }
    }

    public Long zadd(String key, double score, String member) {
        Long rtn;
        Object jedis = null;
        try {
            jedis = getResource();
            if (CLUSTER.equals(redisJedisPool.getPoolType())) {
                rtn = ((JedisCluster) jedis).zadd(key, score, member);
            } else {
                rtn = ((Jedis) jedis).zadd(key, score, member);
            }
            return rtn;
        } catch (Exception e) {
            throw e;
        } finally {
            returnResource(jedis);
        }
    }

    public Long zadd(String key, Map<String, Double> scoreMembers) {
        Long rtn;
        Object jedis = null;
        try {
            jedis = getResource();
            if (CLUSTER.equals(redisJedisPool.getPoolType())) {
                rtn = ((JedisCluster) jedis).zadd(key, scoreMembers);
            } else {
                rtn = ((Jedis) jedis).zadd(key, scoreMembers);
            }
            return rtn;
        } catch (Exception e) {
            throw e;
        } finally {
            returnResource(jedis);
        }
    }

    public Set<String> zrange(String key, long start, long end) {
        Set<String> rtn;
        Object jedis = null;
        try {
            jedis = getResource();
            if (CLUSTER.equals(redisJedisPool.getPoolType())) {
                rtn = ((JedisCluster) jedis).zrange(key, start, end);
            } else {
                rtn = ((Jedis) jedis).zrange(key, start, end);
            }
            return rtn;
        } catch (Exception e) {
            throw e;
        } finally {
            returnResource(jedis);
        }
    }

    public Long zrem(String key, String... members) {
        Long rtn;
        Object jedis = null;
        try {
            jedis = getResource();
            if (CLUSTER.equals(redisJedisPool.getPoolType())) {
                rtn = ((JedisCluster) jedis).zrem(key, members);
            } else {
                rtn = ((Jedis) jedis).zrem(key, members);
            }
            return rtn;
        } catch (Exception e) {
            throw e;
        } finally {
            returnResource(jedis);
        }
    }

    public Double zincrby(String key, double score, String member) {
        Double rtn;
        Object jedis = null;
        try {
            jedis = getResource();
            if (CLUSTER.equals(redisJedisPool.getPoolType())) {
                rtn = ((JedisCluster) jedis).zincrby(key, score, member);
            } else {
                rtn = ((Jedis) jedis).zincrby(key, score, member);
            }
            return rtn;
        } catch (Exception e) {
            throw e;
        } finally {
            returnResource(jedis);
        }
    }

    public Long zrank(String key, String member) {
        Long rtn;
        Object jedis = null;
        try {
            jedis = getResource();
            if (CLUSTER.equals(redisJedisPool.getPoolType())) {
                rtn = ((JedisCluster) jedis).zrank(key, member);
            } else {
                rtn = ((Jedis) jedis).zrank(key, member);
            }
            return rtn;
        } catch (Exception e) {
            throw e;
        } finally {
            returnResource(jedis);
        }
    }

    public Long zrevrank(String key, String member) {
        Long rtn;
        Object jedis = null;
        try {
            jedis = getResource();
            if (CLUSTER.equals(redisJedisPool.getPoolType())) {
                rtn = ((JedisCluster) jedis).zrevrank(key, member);
            } else {
                rtn = ((Jedis) jedis).zrevrank(key, member);
            }
            return rtn;
        } catch (Exception e) {
            throw e;
        } finally {
            returnResource(jedis);
        }
    }

    public Set<String> zrevrange(String key, long start, long end) {
        Set<String> rtn;
        Object jedis = null;
        try {
            jedis = getResource();
            if (CLUSTER.equals(redisJedisPool.getPoolType())) {
                rtn = ((JedisCluster) jedis).zrevrange(key, start, end);
            } else {
                rtn = ((Jedis) jedis).zrevrange(key, start, end);
            }
            return rtn;
        } catch (Exception e) {
            throw e;
        } finally {
            returnResource(jedis);
        }
    }

    public Set<Tuple> zrangeWithScores(String key, long start, long end) {
        Set<Tuple> rtn;
        Object jedis = null;
        try {
            jedis = getResource();
            if (CLUSTER.equals(redisJedisPool.getPoolType())) {
                rtn = ((JedisCluster) jedis).zrangeWithScores(key, start, end);
            } else {
                rtn = ((Jedis) jedis).zrangeWithScores(key, start, end);
            }
            return rtn;
        } catch (Exception e) {
            throw e;
        } finally {
            returnResource(jedis);
        }
    }

    public Set<Tuple> zrevrangeWithScores(String key, long start, long end) {
        Set<Tuple> rtn;
        Object jedis = null;
        try {
            jedis = getResource();
            if (CLUSTER.equals(redisJedisPool.getPoolType())) {
                rtn = ((JedisCluster) jedis).zrevrangeWithScores(key, start, end);
            } else {
                rtn = ((Jedis) jedis).zrevrangeWithScores(key, start, end);
            }
            return rtn;
        } catch (Exception e) {
            throw e;
        } finally {
            returnResource(jedis);
        }
    }

    public Long zcard(String key) {
        Long rtn;
        Object jedis = null;
        try {
            jedis = getResource();
            if (CLUSTER.equals(redisJedisPool.getPoolType())) {
                rtn = ((JedisCluster) jedis).zcard(key);
            } else {
                rtn = ((Jedis) jedis).zcard(key);
            }
            return rtn;
        } catch (Exception e) {
            throw e;
        } finally {
            returnResource(jedis);
        }
    }

    public Double zscore(String key, String member) {
        Double rtn;
        Object jedis = null;
        try {
            jedis = getResource();
            if (CLUSTER.equals(redisJedisPool.getPoolType())) {
                rtn = ((JedisCluster) jedis).zscore(key, member);
            } else {
                rtn = ((Jedis) jedis).zscore(key, member);
            }
            return rtn;
        } catch (Exception e) {
            throw e;
        } finally {
            returnResource(jedis);
        }
    }

    public List<String> sort(String key) {
        List<String> rtn;
        Object jedis = null;
        try {
            jedis = getResource();
            if (CLUSTER.equals(redisJedisPool.getPoolType())) {
                rtn = ((JedisCluster) jedis).sort(key);
            } else {
                rtn = ((Jedis) jedis).sort(key);
            }
            return rtn;
        } catch (Exception e) {
            throw e;
        } finally {
            returnResource(jedis);
        }
    }

    public List<String> sort(String key, SortingParams sortingParameters) {
        List<String> rtn;
        Object jedis = null;
        try {
            jedis = getResource();
            if (CLUSTER.equals(redisJedisPool.getPoolType())) {
                rtn = ((JedisCluster) jedis).sort(key, sortingParameters);
            } else {
                rtn = ((Jedis) jedis).sort(key, sortingParameters);
            }
            return rtn;
        } catch (Exception e) {
            throw e;
        } finally {
            returnResource(jedis);
        }
    }

    public Long zcount(String key, double min, double max) {
        Long rtn;
        Object jedis = null;
        try {
            jedis = getResource();
            if (CLUSTER.equals(redisJedisPool.getPoolType())) {
                rtn = ((JedisCluster) jedis).zcount(key, min, max);
            } else {
                rtn = ((Jedis) jedis).zcount(key, min, max);
            }
            return rtn;
        } catch (Exception e) {
            throw e;
        } finally {
            returnResource(jedis);
        }
    }

    public Long zcount(String key, String min, String max) {
        Long rtn;
        Object jedis = null;
        try {
            jedis = getResource();
            if (CLUSTER.equals(redisJedisPool.getPoolType())) {
                rtn = ((JedisCluster) jedis).zcount(key, min, max);
            } else {
                rtn = ((Jedis) jedis).zcount(key, min, max);
            }
            return rtn;
        } catch (Exception e) {
            throw e;
        } finally {
            returnResource(jedis);
        }
    }

    public Set<String> zrangeByScore(String key, double min, double max) {
        Set<String> rtn;
        Object jedis = null;
        try {
            jedis = getResource();
            if (CLUSTER.equals(redisJedisPool.getPoolType())) {
                rtn = ((JedisCluster) jedis).zrangeByScore(key, min, max);
            } else {
                rtn = ((Jedis) jedis).zrangeByScore(key, min, max);
            }
            return rtn;
        } catch (Exception e) {
            throw e;
        } finally {
            returnResource(jedis);
        }
    }

    public Set<String> zrangeByScore(String key, String min, String max, int offset, int count) {
        Set<String> rtn;
        Object jedis = null;
        try {
            jedis = getResource();
            if (CLUSTER.equals(redisJedisPool.getPoolType())) {
                rtn = ((JedisCluster) jedis).zrangeByScore(key, min, max, offset, count);
            } else {
                rtn = ((Jedis) jedis).zrangeByScore(key, min, max, offset, count);
            }
            return rtn;
        } catch (Exception e) {
            throw e;
        } finally {
            returnResource(jedis);
        }
    }

    public Set<String> zrangeByScore(String key, String min, String max) {
        Set<String> rtn;
        Object jedis = null;
        try {
            jedis = getResource();
            if (CLUSTER.equals(redisJedisPool.getPoolType())) {
                rtn = ((JedisCluster) jedis).zrangeByScore(key, min, max);
            } else {
                rtn = ((Jedis) jedis).zrangeByScore(key, min, max);
            }
            return rtn;
        } catch (Exception e) {
            throw e;
        } finally {
            returnResource(jedis);
        }
    }

    public Set<String> zrevrangeByScore(String key, double max, double min) {
        Set<String> rtn;
        Object jedis = null;
        try {
            jedis = getResource();
            if (CLUSTER.equals(redisJedisPool.getPoolType())) {
                rtn = ((JedisCluster) jedis).zrevrangeByScore(key, max, min);
            } else {
                rtn = ((Jedis) jedis).zrevrangeByScore(key, max, min);
            }
            return rtn;
        } catch (Exception e) {
            throw e;
        } finally {
            returnResource(jedis);
        }
    }

    public Set<String> zrevrangeByScore(String key, String max, String min) {
        Set<String> rtn;
        Object jedis = null;
        try {
            jedis = getResource();
            if (CLUSTER.equals(redisJedisPool.getPoolType())) {
                rtn = ((JedisCluster) jedis).zrevrangeByScore(key, max, min);
            } else {
                rtn = ((Jedis) jedis).zrevrangeByScore(key, max, min);
            }
            return rtn;
        } catch (Exception e) {
            throw e;
        } finally {
            returnResource(jedis);
        }
    }

    public Set<String> zrevrangeByScore(String key, String max, String min, int offset, int count) {
        Set<String> rtn;
        Object jedis = null;
        try {
            jedis = getResource();
            if (CLUSTER.equals(redisJedisPool.getPoolType())) {
                rtn = ((JedisCluster) jedis).zrevrangeByScore(key, max, min, offset, count);
            } else {
                rtn = ((Jedis) jedis).zrevrangeByScore(key, max, min, offset, count);
            }
            return rtn;
        } catch (Exception e) {
            throw e;
        } finally {
            returnResource(jedis);
        }
    }

    public Set<String> zrevrangeByScore(String key, double max, double min, int offset, int count) {
        Set<String> rtn;
        Object jedis = null;
        try {
            jedis = getResource();
            if (CLUSTER.equals(redisJedisPool.getPoolType())) {
                rtn = ((JedisCluster) jedis).zrevrangeByScore(key, max, min, offset, count);
            } else {
                rtn = ((Jedis) jedis).zrevrangeByScore(key, max, min, offset, count);
            }
            return rtn;
        } catch (Exception e) {
            throw e;
        } finally {
            returnResource(jedis);
        }
    }


    public Set<Tuple> zrevrangeByScoreWithScores(String key, double min, double max) {
        Set<Tuple> rtn;
        Object jedis = null;
        try {
            jedis = getResource();
            if (CLUSTER.equals(redisJedisPool.getPoolType())) {
                rtn = ((JedisCluster) jedis).zrevrangeByScoreWithScores(key, min, max);
            } else {
                rtn = ((Jedis) jedis).zrevrangeByScoreWithScores(key, min, max);
            }
            return rtn;
        } catch (Exception e) {
            throw e;
        } finally {
            returnResource(jedis);
        }
    }

    public Set<Tuple> zrevrangeByScoreWithScores(String key, String min, String max) {
        Set<Tuple> rtn;
        Object jedis = null;
        try {
            jedis = getResource();
            if (CLUSTER.equals(redisJedisPool.getPoolType())) {
                rtn = ((JedisCluster) jedis).zrevrangeByScoreWithScores(key, min, max);
            } else {
                rtn = ((Jedis) jedis).zrevrangeByScoreWithScores(key, min, max);
            }
            return rtn;
        } catch (Exception e) {
            throw e;
        } finally {
            returnResource(jedis);
        }
    }

    public Set<Tuple> zrevrangeByScoreWithScores(String key, double min, double max, int offset, int count) {
        Set<Tuple> rtn;
        Object jedis = null;
        try {
            jedis = getResource();
            if (CLUSTER.equals(redisJedisPool.getPoolType())) {
                rtn = ((JedisCluster) jedis).zrevrangeByScoreWithScores(key, min, max, offset, count);
            } else {
                rtn = ((Jedis) jedis).zrevrangeByScoreWithScores(key, min, max, offset, count);
            }
            return rtn;
        } catch (Exception e) {
            throw e;
        } finally {
            returnResource(jedis);
        }
    }

    public Set<Tuple> zrevrangeByScoreWithScores(String key, String min, String max, int offset, int count) {
        Set<Tuple> rtn;
        Object jedis = null;
        try {
            jedis = getResource();
            if (CLUSTER.equals(redisJedisPool.getPoolType())) {
                rtn = ((JedisCluster) jedis).zrevrangeByScoreWithScores(key, min, max, offset, count);
            } else {
                rtn = ((Jedis) jedis).zrevrangeByScoreWithScores(key, min, max, offset, count);
            }
            return rtn;
        } catch (Exception e) {
            throw e;
        } finally {
            returnResource(jedis);
        }
    }

    public Set<Tuple> zrangeByScoreWithScores(String key, double min, double max) {
        Set<Tuple> rtn;
        Object jedis = null;
        try {
            jedis = getResource();
            if (CLUSTER.equals(redisJedisPool.getPoolType())) {
                rtn = ((JedisCluster) jedis).zrangeByScoreWithScores(key, min, max);
            } else {
                rtn = ((Jedis) jedis).zrangeByScoreWithScores(key, min, max);
            }
            return rtn;
        } catch (Exception e) {
            throw e;
        } finally {
            returnResource(jedis);
        }
    }

    public Set<Tuple> zrangeByScoreWithScores(String key, String min, String max, int offset, int count) {
        Set<Tuple> rtn;
        Object jedis = null;
        try {
            jedis = getResource();
            if (CLUSTER.equals(redisJedisPool.getPoolType())) {
                rtn = ((JedisCluster) jedis).zrangeByScoreWithScores(key, min, max, offset, count);
            } else {
                rtn = ((Jedis) jedis).zrangeByScoreWithScores(key, min, max, offset, count);
            }
            return rtn;
        } catch (Exception e) {
            throw e;
        } finally {
            returnResource(jedis);
        }
    }

    public Set<Tuple> zrangeByScoreWithScores(String key, String min, String max) {
        Set<Tuple> rtn;
        Object jedis = null;
        try {
            jedis = getResource();
            if (CLUSTER.equals(redisJedisPool.getPoolType())) {
                rtn = ((JedisCluster) jedis).zrangeByScoreWithScores(key, min, max);
            } else {
                rtn = ((Jedis) jedis).zrangeByScoreWithScores(key, min, max);
            }
            return rtn;
        } catch (Exception e) {
            throw e;
        } finally {
            returnResource(jedis);
        }
    }

    public Long zremrangeByRank(String key, long start, long end) {
        Long rtn;
        Object jedis = null;
        try {
            jedis = getResource();
            if (CLUSTER.equals(redisJedisPool.getPoolType())) {
                rtn = ((JedisCluster) jedis).zremrangeByRank(key, start, end);
            } else {
                rtn = ((Jedis) jedis).zremrangeByRank(key, start, end);
            }
            return rtn;
        } catch (Exception e) {
            throw e;
        } finally {
            returnResource(jedis);
        }
    }

    public Long zremrangeByScore(String key, double start, double end) {
        Long rtn;
        Object jedis = null;
        try {
            jedis = getResource();
            if (CLUSTER.equals(redisJedisPool.getPoolType())) {
                rtn = ((JedisCluster) jedis).zremrangeByScore(key, start, end);
            } else {
                rtn = ((Jedis) jedis).zremrangeByScore(key, start, end);
            }
            return rtn;
        } catch (Exception e) {
            throw e;
        } finally {
            returnResource(jedis);
        }
    }

    public Long zremrangeByScore(String key, String start, String end) {
        Long rtn;
        Object jedis = null;
        try {
            jedis = getResource();
            if (CLUSTER.equals(redisJedisPool.getPoolType())) {
                rtn = ((JedisCluster) jedis).zremrangeByScore(key, start, end);
            } else {
                rtn = ((Jedis) jedis).zremrangeByScore(key, start, end);
            }
            return rtn;
        } catch (Exception e) {
            throw e;
        } finally {
            returnResource(jedis);
        }
    }

    public Long zlexcount(String key, String min, String max) {
        Long rtn;
        Object jedis = null;
        try {
            jedis = getResource();
            if (CLUSTER.equals(redisJedisPool.getPoolType())) {
                rtn = ((JedisCluster) jedis).zlexcount(key, min, max);
            } else {
                rtn = ((Jedis) jedis).zlexcount(key, min, max);
            }
            return rtn;
        } catch (Exception e) {
            throw e;
        } finally {
            returnResource(jedis);
        }
    }

    public Set<String> zrangeByLex(String key, String min, String max) {
        Set<String> rtn;
        Object jedis = null;
        try {
            jedis = getResource();
            if (CLUSTER.equals(redisJedisPool.getPoolType())) {
                rtn = ((JedisCluster) jedis).zrangeByLex(key, min, max);
            } else {
                rtn = ((Jedis) jedis).zrangeByLex(key, min, max);
            }
            return rtn;
        } catch (Exception e) {
            throw e;
        } finally {
            returnResource(jedis);
        }
    }

    public Set<String> zrangeByLex(String key, String min, String max, int offset, int count) {
        Set<String> rtn;
        Object jedis = null;
        try {
            jedis = getResource();
            if (CLUSTER.equals(redisJedisPool.getPoolType())) {
                rtn = ((JedisCluster) jedis).zrangeByLex(key, min, max, offset, count);
            } else {
                rtn = ((Jedis) jedis).zrangeByLex(key, min, max, offset, count);
            }
            return rtn;
        } catch (Exception e) {
            throw e;
        } finally {
            returnResource(jedis);
        }
    }

    public Set<String> zrevrangeByLex(String key, String s1, String s2) {
        Set<String> rtn;
        Object jedis = null;
        try {
            jedis = getResource();
            if (CLUSTER.equals(redisJedisPool.getPoolType())) {
                rtn = ((JedisCluster) jedis).zrevrangeByLex(key, s1, s2);
            } else {
                rtn = ((Jedis) jedis).zrevrangeByLex(key, s1, s2);
            }
            return rtn;
        } catch (Exception e) {
            throw e;
        } finally {
            returnResource(jedis);
        }
    }

    public Set<String> zrevrangeByLex(String key, String s1, String s2, int i, int i1) {
        Set<String> rtn;
        Object jedis = null;
        try {
            jedis = getResource();
            if (CLUSTER.equals(redisJedisPool.getPoolType())) {
                rtn = ((JedisCluster) jedis).zrevrangeByLex(key, s1, s2, i, i1);
            } else {
                rtn = ((Jedis) jedis).zrevrangeByLex(key, s1, s2, i, i1);
            }
            return rtn;
        } catch (Exception e) {
            throw e;
        } finally {
            returnResource(jedis);
        }
    }

    public Long zremrangeByLex(String key, String min, String max) {
        Long rtn;
        Object jedis = null;
        try {
            jedis = getResource();
            if (CLUSTER.equals(redisJedisPool.getPoolType())) {
                rtn = ((JedisCluster) jedis).zremrangeByLex(key, min, max);
            } else {
                rtn = ((Jedis) jedis).zremrangeByLex(key, min, max);
            }
            return rtn;
        } catch (Exception e) {
            throw e;
        } finally {
            returnResource(jedis);
        }
    }

    public Long lpushx(String key, String... string) {
        Long rtn;
        Object jedis = null;
        try {
            jedis = getResource();
            if (CLUSTER.equals(redisJedisPool.getPoolType())) {
                rtn = ((JedisCluster) jedis).lpushx(key, string);
            } else {
                rtn = ((Jedis) jedis).lpushx(key, string);
            }
            return rtn;
        } catch (Exception e) {
            throw e;
        } finally {
            returnResource(jedis);
        }
    }

    public Long rpushx(String key, String... string) {
        Long rtn;
        Object jedis = null;
        try {
            jedis = getResource();
            if (CLUSTER.equals(redisJedisPool.getPoolType())) {
                rtn = ((JedisCluster) jedis).rpushx(key, string);
            } else {
                rtn = ((Jedis) jedis).rpushx(key, string);
            }
            return rtn;
        } catch (Exception e) {
            throw e;
        } finally {
            returnResource(jedis);
        }
    }

    public List<String> blpop(String arg) {
        List<String> rtn;
        Object jedis = null;
        try {
            jedis = getResource();
            if (CLUSTER.equals(redisJedisPool.getPoolType())) {
                rtn = ((JedisCluster) jedis).blpop(Integer.MAX_VALUE, arg);
            } else {
                rtn = ((Jedis) jedis).blpop(arg);
            }
            return rtn;
        } catch (Exception e) {
            throw e;
        } finally {
            returnResource(jedis);
        }
    }

    public List<String> blpop(int timeout, String key) {
        List<String> rtn;
        Object jedis = null;
        try {
            jedis = getResource();
            if (CLUSTER.equals(redisJedisPool.getPoolType())) {
                rtn = ((JedisCluster) jedis).blpop(timeout, key);
            } else {
                rtn = ((Jedis) jedis).blpop(timeout, key);
            }
            return rtn;
        } catch (Exception e) {
            throw e;
        } finally {
            returnResource(jedis);
        }
    }

    public List<String> brpop(String arg) {
        List<String> rtn;
        Object jedis = null;
        try {
            jedis = getResource();
            if (CLUSTER.equals(redisJedisPool.getPoolType())) {
                rtn = ((JedisCluster) jedis).brpop(Integer.MAX_VALUE, arg);
            } else {
                rtn = ((Jedis) jedis).brpop(arg);
            }
            return rtn;
        } catch (Exception e) {
            throw e;
        } finally {
            returnResource(jedis);
        }
    }

    public List<String> brpop(int timeout, String key) {
        List<String> rtn;
        Object jedis = null;
        try {
            jedis = getResource();
            if (CLUSTER.equals(redisJedisPool.getPoolType())) {
                rtn = ((JedisCluster) jedis).brpop(timeout, key);
            } else {
                rtn = ((Jedis) jedis).brpop(timeout, key);
            }
            return rtn;
        } catch (Exception e) {
            throw e;
        } finally {
            returnResource(jedis);
        }
    }

    public Long del(String key) {
        Long rtn;
        Object jedis = null;
        try {
            jedis = getResource();
            if (CLUSTER.equals(redisJedisPool.getPoolType())) {
                rtn = ((JedisCluster) jedis).del(key);
            } else {
                rtn = ((Jedis) jedis).del(key);
            }
            return rtn;
        } catch (Exception e) {
            throw e;
        } finally {
            returnResource(jedis);
        }
    }

    public String echo(String string) {
        String rtn;
        Object jedis = null;
        try {
            jedis = getResource();
            if (CLUSTER.equals(redisJedisPool.getPoolType())) {
                rtn = ((JedisCluster) jedis).echo(string);
            } else {
                rtn = ((Jedis) jedis).echo(string);
            }
            return rtn;
        } catch (Exception e) {
            throw e;
        } finally {
            returnResource(jedis);
        }
    }

    public Long move(String key, int dbIndex) {
        Long rtn;
        Object jedis = null;
        try {
            jedis = getResource();
            if (CLUSTER.equals(redisJedisPool.getPoolType())) {
                //                rtn = ((JedisCluster)jedis).move(key, dbIndex);
                throw new IllegalStateException("not support");
            } else {
                rtn = ((Jedis) jedis).move(key, dbIndex);
            }
            return rtn;
        } catch (Exception e) {
            throw e;
        } finally {
            returnResource(jedis);
        }
    }

    public Long bitcount(String key) {
        Long rtn;
        Object jedis = null;
        try {
            jedis = getResource();
            if (CLUSTER.equals(redisJedisPool.getPoolType())) {
                rtn = ((JedisCluster) jedis).bitcount(key);
            } else {
                rtn = ((Jedis) jedis).bitcount(key);
            }
            return rtn;
        } catch (Exception e) {
            throw e;
        } finally {
            returnResource(jedis);
        }
    }

    public Long bitcount(String key, long start, long end) {
        Long rtn;
        Object jedis = null;
        try {
            jedis = getResource();
            if (CLUSTER.equals(redisJedisPool.getPoolType())) {
                rtn = ((JedisCluster) jedis).bitcount(key, start, end);
            } else {
                rtn = ((Jedis) jedis).bitcount(key, start, end);
            }
            return rtn;
        } catch (Exception e) {
            throw e;
        } finally {
            returnResource(jedis);
        }
    }

    public Long bitpos(String key, boolean b) {
        Long rtn;
        Object jedis = null;
        try {
            jedis = getResource();
            if (CLUSTER.equals(redisJedisPool.getPoolType())) {
                //                rtn = ((JedisCluster)jedis).bitpos(key,b);
                throw new IllegalStateException("not support");
            } else {
                rtn = ((Jedis) jedis).bitpos(key, b);
            }
            return rtn;
        } catch (Exception e) {
            throw e;
        } finally {
            returnResource(jedis);
        }
    }

    public Long bitpos(String key, boolean b, BitPosParams bitPosParams) {
        Long rtn;
        Object jedis = null;
        try {
            jedis = getResource();
            if (CLUSTER.equals(redisJedisPool.getPoolType())) {
                //                rtn = ((JedisCluster)jedis).bitpos(key,b,bitPosParams);
                throw new IllegalStateException("not support");
            } else {
                rtn = ((Jedis) jedis).bitpos(key, b, bitPosParams);
            }
            return rtn;
        } catch (Exception e) {
            throw e;
        } finally {
            returnResource(jedis);
        }
    }

    public ScanResult<Map.Entry<String, String>> hscan(String key, int cursor) {
        ScanResult<Map.Entry<String, String>> rtn;
        Object jedis = null;
        try {
            jedis = getResource();
            if (CLUSTER.equals(redisJedisPool.getPoolType())) {
                rtn = ((JedisCluster) jedis).hscan(key, "" + cursor);
            } else {
                rtn = ((Jedis) jedis).hscan(key, "" + cursor);
            }
            return rtn;
        } catch (Exception e) {
            throw e;
        } finally {
            returnResource(jedis);
        }
    }

    public ScanResult<String> sscan(String key, int cursor) {
        ScanResult<String> rtn;
        Object jedis = null;
        try {
            jedis = getResource();
            if (CLUSTER.equals(redisJedisPool.getPoolType())) {
                rtn = ((JedisCluster) jedis).sscan(key, "" + cursor);
            } else {
                rtn = ((Jedis) jedis).sscan(key, "" + cursor);
            }
            return rtn;
        } catch (Exception e) {
            throw e;
        } finally {
            returnResource(jedis);
        }
    }

    public ScanResult<Tuple> zscan(String key, int cursor) {
        ScanResult<Tuple> rtn;
        Object jedis = null;
        try {
            jedis = getResource();
            if (CLUSTER.equals(redisJedisPool.getPoolType())) {
                rtn = ((JedisCluster) jedis).zscan(key, "" + cursor);
            } else {
                rtn = ((Jedis) jedis).zscan(key, "" + cursor);
            }
            return rtn;
        } catch (Exception e) {
            throw e;
        } finally {
            returnResource(jedis);
        }
    }

    public ScanResult<Map.Entry<String, String>> hscan(String key, String cursor) {
        ScanResult<Map.Entry<String, String>> rtn;
        Object jedis = null;
        try {
            jedis = getResource();
            if (CLUSTER.equals(redisJedisPool.getPoolType())) {
                rtn = ((JedisCluster) jedis).hscan(key, cursor);
            } else {
                rtn = ((Jedis) jedis).hscan(key, cursor);
            }
            return rtn;
        } catch (Exception e) {
            throw e;
        } finally {
            returnResource(jedis);
        }
    }

    public ScanResult<Map.Entry<String, String>> hscan(String key, String s1, ScanParams scanParams)
            throws UnsupportedEncodingException {
        ScanResult<Map.Entry<String, String>> rtn;
        Object jedis = null;
        try {
            jedis = getResource();
            if (CLUSTER.equals(redisJedisPool.getPoolType())) {
                rtn = ((JedisCluster) jedis).hscan(key, s1, scanParams);
            } else {
                rtn = ((Jedis) jedis).hscan(key, s1, scanParams);
            }
            return rtn;
        } catch (Exception e) {
            throw e;
        } finally {
            returnResource(jedis);
        }
    }

    public ScanResult<String> sscan(String key, String cursor) {
        ScanResult<String> rtn;
        Object jedis = null;
        try {
            jedis = getResource();
            if (CLUSTER.equals(redisJedisPool.getPoolType())) {
                rtn = ((JedisCluster) jedis).sscan(key, cursor);
            } else {
                rtn = ((Jedis) jedis).sscan(key, cursor);
            }
            return rtn;
        } catch (Exception e) {
            throw e;
        } finally {
            returnResource(jedis);
        }
    }

    public ScanResult<String> sscan(String key, String s1, ScanParams scanParams) throws UnsupportedEncodingException {
        ScanResult<String> rtn;
        Object jedis = null;
        try {
            jedis = getResource();
            if (CLUSTER.equals(redisJedisPool.getPoolType())) {
                rtn = ((JedisCluster) jedis).sscan(key, s1, scanParams);
            } else {
                rtn = ((Jedis) jedis).sscan(key, s1, scanParams);
            }
            return rtn;
        } catch (Exception e) {
            throw e;
        } finally {
            returnResource(jedis);
        }
    }

    public ScanResult<Tuple> zscan(String key, String cursor) {
        ScanResult<Tuple> rtn;
        Object jedis = null;
        try {
            jedis = getResource();
            if (CLUSTER.equals(redisJedisPool.getPoolType())) {
                rtn = ((JedisCluster) jedis).zscan(key, cursor);
            } else {
                rtn = ((Jedis) jedis).zscan(key, cursor);
            }
            return rtn;
        } catch (Exception e) {
            throw e;
        } finally {
            returnResource(jedis);
        }
    }

    public ScanResult<Tuple> zscan(String key, String s1, ScanParams scanParams) throws UnsupportedEncodingException {
        ScanResult<Tuple> rtn;
        Object jedis = null;
        try {
            jedis = getResource();
            if (CLUSTER.equals(redisJedisPool.getPoolType())) {
                rtn = ((JedisCluster) jedis).zscan(key, s1, scanParams);
            } else {
                rtn = ((Jedis) jedis).zscan(key, s1, scanParams);
            }
            return rtn;
        } catch (Exception e) {
            throw e;
        } finally {
            returnResource(jedis);
        }
    }

    public Long pfadd(String key, String... elements) {
        Long rtn;
        Object jedis = null;
        try {
            jedis = getResource();
            if (CLUSTER.equals(redisJedisPool.getPoolType())) {
                rtn = ((JedisCluster) jedis).pfadd(key, elements);
            } else {
                rtn = ((Jedis) jedis).pfadd(key, elements);
            }
            return rtn;
        } catch (Exception e) {
            throw e;
        } finally {
            returnResource(jedis);
        }
    }

    public long pfcount(String key) {
        Long rtn;
        Object jedis = null;
        try {
            jedis = getResource();
            if (CLUSTER.equals(redisJedisPool.getPoolType())) {
                rtn = ((JedisCluster) jedis).pfcount(key);
            } else {
                rtn = ((Jedis) jedis).pfcount(key);
            }
            return rtn;
        } catch (Exception e) {
            throw e;
        } finally {
            returnResource(jedis);
        }
    }

    public Long geoadd(String key, double v, double v1, String s1) {
        Long rtn;
        Object jedis = null;
        try {
            jedis = getResource();
            if (CLUSTER.equals(redisJedisPool.getPoolType())) {
                rtn = ((JedisCluster) jedis).geoadd(key, v, v1, s1);
            } else {
                rtn = ((Jedis) jedis).geoadd(key, v, v1, s1);
            }
            return rtn;
        } catch (Exception e) {
            throw e;
        } finally {
            returnResource(jedis);
        }
    }

    public Long geoadd(String key, Map<String, GeoCoordinate> map) {
        Long rtn;
        Object jedis = null;
        try {
            jedis = getResource();
            if (CLUSTER.equals(redisJedisPool.getPoolType())) {
                rtn = ((JedisCluster) jedis).geoadd(key, map);
            } else {
                rtn = ((Jedis) jedis).geoadd(key, map);
            }
            return rtn;
        } catch (Exception e) {
            throw e;
        } finally {
            returnResource(jedis);
        }
    }

    public Double geodist(String key, String s1, String s2) {
        Double rtn;
        Object jedis = null;
        try {
            jedis = getResource();
            if (CLUSTER.equals(redisJedisPool.getPoolType())) {
                rtn = ((JedisCluster) jedis).geodist(key, s1, s2);
            } else {
                rtn = ((Jedis) jedis).geodist(key, s1, s2);
            }
            return rtn;
        } catch (Exception e) {
            throw e;
        } finally {
            returnResource(jedis);
        }
    }

    public Double geodist(String key, String s1, String s2, GeoUnit geoUnit) {
        Double rtn;
        Object jedis = null;
        try {
            jedis = getResource();
            if (CLUSTER.equals(redisJedisPool.getPoolType())) {
                rtn = ((JedisCluster) jedis).geodist(key, s1, s2, geoUnit);
            } else {
                rtn = ((Jedis) jedis).geodist(key, s1, s2, geoUnit);
            }
            return rtn;
        } catch (Exception e) {
            throw e;
        } finally {
            returnResource(jedis);
        }
    }

    public List<String> geohash(String key, String... strings) {
        List<String> rtn;
        Object jedis = null;
        try {
            jedis = getResource();
            if (CLUSTER.equals(redisJedisPool.getPoolType())) {
                rtn = ((JedisCluster) jedis).geohash(key, strings);
            } else {
                rtn = ((Jedis) jedis).geohash(key, strings);
            }
            return rtn;
        } catch (Exception e) {
            throw e;
        } finally {
            returnResource(jedis);
        }
    }

    public List<GeoCoordinate> geopos(String key, String... strings) {
        List<GeoCoordinate> rtn;
        Object jedis = null;
        try {
            jedis = getResource();
            if (CLUSTER.equals(redisJedisPool.getPoolType())) {
                rtn = ((JedisCluster) jedis).geopos(key, strings);
            } else {
                rtn = ((Jedis) jedis).geopos(key, strings);
            }
            return rtn;
        } catch (Exception e) {
            throw e;
        } finally {
            returnResource(jedis);
        }
    }

    public List<GeoRadiusResponse> georadius(String key, double v, double v1, double v2, GeoUnit geoUnit) {
        List<GeoRadiusResponse> rtn;
        Object jedis = null;
        try {
            jedis = getResource();
            if (CLUSTER.equals(redisJedisPool.getPoolType())) {
                rtn = ((JedisCluster) jedis).georadius(key, v, v1, v2, geoUnit);
            } else {
                rtn = ((Jedis) jedis).georadius(key, v, v1, v2, geoUnit);
            }
            return rtn;
        } catch (Exception e) {
            throw e;
        } finally {
            returnResource(jedis);
        }
    }

    /*public List<GeoRadiusResponse> georadius(String key, double v, double v1, double v2, GeoUnit geoUnit, GeoRadiusParam geoRadiusParam) {
        List<GeoRadiusResponse>  rtn;
        Object jedis = null;
        try{
            jedis = getResource();
            if(MASTER.equals(redisJedisPool.getPoolType())){
                rtn = ((MasterSlaveJedis)jedis).georadius(key, v, v1, v2, geoUnit, geoRadiusParam);
            }else if(CLUSTER.equals(redisJedisPool.getPoolType())){
                rtn = ((JedisCluster)jedis).georadius(key, v, v1, v2, geoUnit, geoRadiusParam);
            }else{
                rtn = ((Jedis) jedis).georadius(key, v, v1, v2, geoUnit, geoRadiusParam);
            }
            return rtn;
        }catch(Exception e){
            throw e;
        }finally {
            returnResource(jedis);
        }
    }*/
    public List<GeoRadiusResponse> georadiusByMember(String key, String s1, double v, GeoUnit geoUnit) {
        List<GeoRadiusResponse> rtn;
        Object jedis = null;
        try {
            jedis = getResource();
            if (CLUSTER.equals(redisJedisPool.getPoolType())) {
                rtn = ((JedisCluster) jedis).georadiusByMember(key, s1, v, geoUnit);
            } else {
                rtn = ((Jedis) jedis).georadiusByMember(key, s1, v, geoUnit);
            }
            return rtn;
        } catch (Exception e) {
            throw e;
        } finally {
            returnResource(jedis);
        }
    }

    /*public List<GeoRadiusResponse> georadiusByMember(String key, String s1, double v, GeoUnit geoUnit, GeoRadiusParam geoRadiusParam) {
        List<GeoRadiusResponse>  rtn;
        Object jedis = null;
        try{
            jedis = getResource();
            if(MASTER.equals(redisJedisPool.getPoolType())){
                rtn = ((MasterSlaveJedis)jedis).georadiusByMember(key, s1, v, geoUnit, geoRadiusParam);
            }else if(CLUSTER.equals(redisJedisPool.getPoolType())){
                rtn = ((JedisCluster)jedis).georadiusByMember(key, s1, v, geoUnit, geoRadiusParam);
            }else{
                rtn = ((Jedis) jedis).georadiusByMember(key, s1, v, geoUnit, geoRadiusParam);
            }
            return rtn;
        }catch(Exception e){
            throw e;
        }finally {
            returnResource(jedis);
        }
    }*/
    public Object eval(byte[] script, byte[] keyCount, byte[]... params) {
        Object rtn;
        Object jedis = null;
        try {
            jedis = getResource();
            if (CLUSTER.equals(redisJedisPool.getPoolType())) {
                rtn = ((JedisCluster) jedis).eval(script, keyCount, params);
            } else {
                rtn = ((Jedis) jedis).eval(script, keyCount, params);
            }
            return rtn;
        } catch (Exception e) {
            throw e;
        } finally {
            returnResource(jedis);
        }
    }

    public Object eval(byte[] script, int keyCount, byte[]... params) {
        Object rtn;
        Object jedis = null;
        try {
            jedis = getResource();
            if (CLUSTER.equals(redisJedisPool.getPoolType())) {
                rtn = ((JedisCluster) jedis).eval(script, keyCount, params);
            } else {
                rtn = ((Jedis) jedis).eval(script, keyCount, params);
            }
            return rtn;
        } catch (Exception e) {
            throw e;
        } finally {
            returnResource(jedis);
        }
    }

    public Object eval(byte[] script, List<byte[]> keys, List<byte[]> args) {
        Object rtn;
        Object jedis = null;
        try {
            jedis = getResource();
            if (CLUSTER.equals(redisJedisPool.getPoolType())) {
                rtn = ((JedisCluster) jedis).eval(script, keys, args);
            } else {
                rtn = ((Jedis) jedis).eval(script, keys, args);
            }
            return rtn;
        } catch (Exception e) {
            throw e;
        } finally {
            returnResource(jedis);
        }
    }

    public Object eval(byte[] script) {
        Object rtn;
        Object jedis = null;
        try {
            jedis = getResource();
            if (CLUSTER.equals(redisJedisPool.getPoolType())) {
                rtn = ((JedisCluster) jedis).eval(script, 0);
            } else {
                rtn = ((Jedis) jedis).eval(script);
            }
            return rtn;
        } catch (Exception e) {
            throw e;
        } finally {
            returnResource(jedis);
        }
    }

    public Object evalsha(byte[] script) {
        Object rtn;
        Object jedis = null;
        try {
            jedis = getResource();
            if (CLUSTER.equals(redisJedisPool.getPoolType())) {
                rtn = ((JedisCluster) jedis).evalsha(script, 1);
            } else {
                rtn = ((Jedis) jedis).evalsha(script);
            }
            return rtn;
        } catch (Exception e) {
            throw e;
        } finally {
            returnResource(jedis);
        }
    }

    public Object evalsha(byte[] sha1, List<byte[]> keys, List<byte[]> args) {
        Object rtn;
        Object jedis = null;
        try {
            jedis = getResource();
            if (CLUSTER.equals(redisJedisPool.getPoolType())) {
                rtn = ((JedisCluster) jedis).evalsha(sha1, keys, args);
            } else {
                rtn = ((Jedis) jedis).evalsha(sha1, keys, args);
            }
            return rtn;
        } catch (Exception e) {
            throw e;
        } finally {
            returnResource(jedis);
        }
    }

    public Object evalsha(byte[] sha1, int keyCount, byte[]... params) {
        Object rtn;
        Object jedis = null;
        try {
            jedis = getResource();
            if (CLUSTER.equals(redisJedisPool.getPoolType())) {
                rtn = ((JedisCluster) jedis).evalsha(sha1, keyCount, params);
            } else {
                rtn = ((Jedis) jedis).evalsha(sha1, keyCount, params);
            }
            return rtn;
        } catch (Exception e) {
            throw e;
        } finally {
            returnResource(jedis);
        }
    }

    public List<Long> scriptExists(byte[]... sha1) throws UnsupportedEncodingException {
        List<Long> rtn;
        Object jedis = null;
        try {
            jedis = getResource();
            if (CLUSTER.equals(redisJedisPool.getPoolType())) {
                rtn = ((JedisCluster) jedis).scriptExists(scriptKey.getBytes("utf-8"), sha1);
            } else {
                rtn = ((Jedis) jedis).scriptExists(sha1);
            }
            return rtn;
        } catch (Exception e) {
            throw e;
        } finally {
            returnResource(jedis);
        }
    }

    public byte[] scriptLoad(byte[] script) throws UnsupportedEncodingException {
        byte[] rtn;
        Object jedis = null;
        try {
            jedis = getResource();
            if (CLUSTER.equals(redisJedisPool.getPoolType())) {
                rtn = ((JedisCluster) jedis).scriptLoad(script, scriptKey.getBytes("utf-8"));
            } else {
                rtn = ((Jedis) jedis).scriptLoad(script);
            }
            return rtn;
        } catch (Exception e) {
            throw e;
        } finally {
            returnResource(jedis);
        }
    }

    public String scriptFlush() throws UnsupportedEncodingException {
        String rtn;
        Object jedis = null;
        try {
            jedis = getResource();
            if (CLUSTER.equals(redisJedisPool.getPoolType())) {
                rtn = ((JedisCluster) jedis).scriptFlush(scriptKey.getBytes("utf-8"));
            } else {
                rtn = ((Jedis) jedis).scriptFlush();
            }
            return rtn;
        } catch (Exception e) {
            throw e;
        } finally {
            returnResource(jedis);
        }
    }

    public String scriptKill() throws UnsupportedEncodingException {
        String rtn;
        Object jedis = null;
        try {
            jedis = getResource();
            if (CLUSTER.equals(redisJedisPool.getPoolType())) {
                rtn = ((JedisCluster) jedis).scriptKill(scriptKey.getBytes("utf-8"));
            } else {
                rtn = ((Jedis) jedis).scriptKill();
            }
            return rtn;
        } catch (Exception e) {
            throw e;
        } finally {
            returnResource(jedis);
        }
    }

    public List<byte[]> configGet(byte[] pattern) {
        List<byte[]> rtn;
        Object jedis = null;
        try {
            jedis = getResource();
            if (CLUSTER.equals(redisJedisPool.getPoolType())) {
                //                rtn = ((JedisCluster)jedis).configGet(pattern);
                throw new IllegalStateException("not support");
            } else {
                rtn = ((Jedis) jedis).configGet(pattern);
            }
            return rtn;
        } catch (Exception e) {
            throw e;
        } finally {
            returnResource(jedis);
        }
    }

    public byte[] configSet(byte[] parameter, byte[] value) {
        byte[] rtn;
        Object jedis = null;
        try {
            jedis = getResource();
            if (CLUSTER.equals(redisJedisPool.getPoolType())) {
                //                rtn = ((JedisCluster)jedis).configSet(parameter, value);
                throw new IllegalStateException("not support");
            } else {
                rtn = ((Jedis) jedis).configSet(parameter, value);
            }
            return rtn;
        } catch (Exception e) {
            throw e;
        } finally {
            returnResource(jedis);
        }
    }

    public String slowlogReset() {
        String rtn;
        Object jedis = null;
        try {
            jedis = getResource();
            if (CLUSTER.equals(redisJedisPool.getPoolType())) {
                //                rtn = ((JedisCluster)jedis).slowlogReset();
                throw new IllegalStateException("not support");
            } else {
                rtn = ((Jedis) jedis).slowlogReset();
            }
            return rtn;
        } catch (Exception e) {
            throw e;
        } finally {
            returnResource(jedis);
        }
    }

    public Long slowlogLen() {
        Long rtn;
        Object jedis = null;
        try {
            jedis = getResource();
            if (CLUSTER.equals(redisJedisPool.getPoolType())) {
                //                rtn = ((JedisCluster)jedis).slowlogLen();
                throw new IllegalStateException("not support");
            } else {
                rtn = ((Jedis) jedis).slowlogLen();
            }
            return rtn;
        } catch (Exception e) {
            throw e;
        } finally {
            returnResource(jedis);
        }
    }

    public List<byte[]> slowlogGetBinary() {
        List<byte[]> rtn;
        Object jedis = null;
        try {
            jedis = getResource();
            if (CLUSTER.equals(redisJedisPool.getPoolType())) {
                //                rtn = ((JedisCluster)jedis).slowlogGetBinary();
                throw new IllegalStateException("not support");
            } else {
                rtn = ((Jedis) jedis).slowlogGetBinary();
            }
            return rtn;
        } catch (Exception e) {
            throw e;
        } finally {
            returnResource(jedis);
        }
    }

    public List<byte[]> slowlogGetBinary(long entries) {
        List<byte[]> rtn;
        Object jedis = null;
        try {
            jedis = getResource();
            if (CLUSTER.equals(redisJedisPool.getPoolType())) {
                //                rtn = ((JedisCluster)jedis).slowlogGetBinary(entries);
                throw new IllegalStateException("not support");
            } else {
                rtn = ((Jedis) jedis).slowlogGetBinary(entries);
            }
            return rtn;
        } catch (Exception e) {
            throw e;
        } finally {
            returnResource(jedis);
        }
    }

    public Long objectRefcount(byte[] key) {
        Long rtn;
        Object jedis = null;
        try {
            jedis = getResource();
            if (CLUSTER.equals(redisJedisPool.getPoolType())) {
                //                rtn = ((JedisCluster)jedis).objectRefcount(key);
                throw new IllegalStateException("not support");
            } else {
                rtn = ((Jedis) jedis).objectRefcount(key);
            }
            return rtn;
        } catch (Exception e) {
            throw e;
        } finally {
            returnResource(jedis);
        }
    }

    public byte[] objectEncoding(byte[] key) {
        byte[] rtn;
        Object jedis = null;
        try {
            jedis = getResource();
            if (CLUSTER.equals(redisJedisPool.getPoolType())) {
                //                rtn = ((JedisCluster)jedis).objectEncoding(key);
                throw new IllegalStateException("not support");
            } else {
                rtn = ((Jedis) jedis).objectEncoding(key);
            }
            return rtn;
        } catch (Exception e) {
            throw e;
        } finally {
            returnResource(jedis);
        }
    }

    public Long objectIdletime(byte[] key) {
        Long rtn;
        Object jedis = null;
        try {
            jedis = getResource();
            if (CLUSTER.equals(redisJedisPool.getPoolType())) {
                //                rtn = ((JedisCluster)jedis).objectIdletime(key);
                throw new IllegalStateException("not support");
            } else {
                rtn = ((Jedis) jedis).objectIdletime(key);
            }
            return rtn;
        } catch (Exception e) {
            throw e;
        } finally {
            returnResource(jedis);
        }
    }

    public Long del(byte[]... keys) {
        Long rtn;
        Object jedis = null;
        try {
            jedis = getResource();
            if (CLUSTER.equals(redisJedisPool.getPoolType())) {
                rtn = ((JedisCluster) jedis).del(keys);
            } else {
                rtn = ((Jedis) jedis).del(keys);
            }
            return rtn;
        } catch (Exception e) {
            throw e;
        } finally {
            returnResource(jedis);
        }
    }

    public Long exists(byte[]... bytes) {
        Long rtn;
        Object jedis = null;
        try {
            jedis = getResource();
            if (CLUSTER.equals(redisJedisPool.getPoolType())) {
                rtn = ((JedisCluster) jedis).exists(bytes);
            } else {
                rtn = ((Jedis) jedis).exists(bytes);
            }
            return rtn;
        } catch (Exception e) {
            throw e;
        } finally {
            returnResource(jedis);
        }
    }

    public List<byte[]> blpop(int timeout, byte[]... keys) {
        List<byte[]> rtn;
        Object jedis = null;
        try {
            jedis = getResource();
            if (CLUSTER.equals(redisJedisPool.getPoolType())) {
                rtn = ((JedisCluster) jedis).blpop(timeout, keys);
            } else {
                rtn = ((Jedis) jedis).blpop(timeout, keys);
            }
            return rtn;
        } catch (Exception e) {
            throw e;
        } finally {
            returnResource(jedis);
        }
    }

    public List<byte[]> brpop(int timeout, byte[]... keys) {
        List<byte[]> rtn;
        Object jedis = null;
        try {
            jedis = getResource();
            if (CLUSTER.equals(redisJedisPool.getPoolType())) {
                rtn = ((JedisCluster) jedis).brpop(timeout, keys);
            } else {
                rtn = ((Jedis) jedis).brpop(timeout, keys);
            }
            return rtn;
        } catch (Exception e) {
            throw e;
        } finally {
            returnResource(jedis);
        }
    }

    public List<byte[]> blpop(byte[]... args) {
        List<byte[]> rtn;
        Object jedis = null;
        try {
            jedis = getResource();
            if (CLUSTER.equals(redisJedisPool.getPoolType())) {
                rtn = ((JedisCluster) jedis).blpop(Integer.MAX_VALUE, args);
            } else {
                rtn = ((Jedis) jedis).blpop(args);
            }
            return rtn;
        } catch (Exception e) {
            throw e;
        } finally {
            returnResource(jedis);
        }
    }

    public List<byte[]> brpop(byte[]... args) {
        List<byte[]> rtn;
        Object jedis = null;
        try {
            jedis = getResource();
            if (CLUSTER.equals(redisJedisPool.getPoolType())) {
                rtn = ((JedisCluster) jedis).brpop(Integer.MAX_VALUE, args);
            } else {
                rtn = ((Jedis) jedis).brpop(args);
            }
            return rtn;
        } catch (Exception e) {
            throw e;
        } finally {
            returnResource(jedis);
        }
    }

    public Set<byte[]> keys(byte[] pattern) {

        return null;
    }

    public List<byte[]> mget(byte[]... keys) {
        List<byte[]> rtn;
        Object jedis = null;
        try {
            jedis = getResource();
            if (CLUSTER.equals(redisJedisPool.getPoolType())) {
                rtn = ((JedisCluster) jedis).mget(keys);
            } else {
                rtn = ((Jedis) jedis).mget(keys);
            }
            return rtn;
        } catch (Exception e) {
            throw e;
        } finally {
            returnResource(jedis);
        }
    }

    public String mset(byte[]... keysvalues) {
        String rtn;
        Object jedis = null;
        try {
            jedis = getResource();
            if (CLUSTER.equals(redisJedisPool.getPoolType())) {
                rtn = ((JedisCluster) jedis).mset(keysvalues);
            } else {
                rtn = ((Jedis) jedis).mset(keysvalues);
            }
            return rtn;
        } catch (Exception e) {
            throw e;
        } finally {
            returnResource(jedis);
        }
    }

    public Long msetnx(byte[]... keysvalues) {
        Long rtn;
        Object jedis = null;
        try {
            jedis = getResource();
            if (CLUSTER.equals(redisJedisPool.getPoolType())) {
                rtn = ((JedisCluster) jedis).msetnx(keysvalues);
            } else {
                rtn = ((Jedis) jedis).msetnx(keysvalues);
            }
            return rtn;
        } catch (Exception e) {
            throw e;
        } finally {
            returnResource(jedis);
        }
    }

    public String rename(byte[] oldkey, byte[] newkey) {
        String rtn;
        Object jedis = null;
        try {
            jedis = getResource();
            if (CLUSTER.equals(redisJedisPool.getPoolType())) {
                rtn = ((JedisCluster) jedis).rename(oldkey, newkey);
            } else {
                rtn = ((Jedis) jedis).rename(oldkey, newkey);
            }
            return rtn;
        } catch (Exception e) {
            throw e;
        } finally {
            returnResource(jedis);
        }
    }

    public Long renamenx(byte[] oldkey, byte[] newkey) {
        Long rtn;
        Object jedis = null;
        try {
            jedis = getResource();
            if (CLUSTER.equals(redisJedisPool.getPoolType())) {
                rtn = ((JedisCluster) jedis).renamenx(oldkey, newkey);
            } else {
                rtn = ((Jedis) jedis).renamenx(oldkey, newkey);
            }
            return rtn;
        } catch (Exception e) {
            throw e;
        } finally {
            returnResource(jedis);
        }
    }

    public byte[] rpoplpush(byte[] srckey, byte[] dstkey) {
        byte[] rtn;
        Object jedis = null;
        try {
            jedis = getResource();
            if (CLUSTER.equals(redisJedisPool.getPoolType())) {
                rtn = ((JedisCluster) jedis).rpoplpush(srckey, dstkey);
            } else {
                rtn = ((Jedis) jedis).rpoplpush(srckey, dstkey);
            }
            return rtn;
        } catch (Exception e) {
            throw e;
        } finally {
            returnResource(jedis);
        }
    }

    public Set<byte[]> sdiff(byte[]... keys) {
        Set<byte[]> rtn;
        Object jedis = null;
        try {
            jedis = getResource();
            if (CLUSTER.equals(redisJedisPool.getPoolType())) {
                rtn = ((JedisCluster) jedis).sdiff(keys);
            } else {
                rtn = ((Jedis) jedis).sdiff(keys);
            }
            return rtn;
        } catch (Exception e) {
            throw e;
        } finally {
            returnResource(jedis);
        }
    }

    public Long sdiffstore(byte[] dstkey, byte[]... keys) {
        Long rtn;
        Object jedis = null;
        try {
            jedis = getResource();
            if (CLUSTER.equals(redisJedisPool.getPoolType())) {
                rtn = ((JedisCluster) jedis).sdiffstore(dstkey, keys);
            } else {
                rtn = ((Jedis) jedis).sdiffstore(dstkey, keys);
            }
            return rtn;
        } catch (Exception e) {
            throw e;
        } finally {
            returnResource(jedis);
        }
    }

    public Set<byte[]> sinter(byte[]... keys) {
        Set<byte[]> rtn;
        Object jedis = null;
        try {
            jedis = getResource();
            if (CLUSTER.equals(redisJedisPool.getPoolType())) {
                rtn = ((JedisCluster) jedis).sinter(keys);
            } else {
                rtn = ((Jedis) jedis).sinter(keys);
            }
            return rtn;
        } catch (Exception e) {
            throw e;
        } finally {
            returnResource(jedis);
        }
    }

    public Long sinterstore(byte[] dstkey, byte[]... keys) {
        Long rtn;
        Object jedis = null;
        try {
            jedis = getResource();
            if (CLUSTER.equals(redisJedisPool.getPoolType())) {
                rtn = ((JedisCluster) jedis).sinterstore(dstkey, keys);
            } else {
                rtn = ((Jedis) jedis).sinterstore(dstkey, keys);
            }
            return rtn;
        } catch (Exception e) {
            throw e;
        } finally {
            returnResource(jedis);
        }
    }

    public Long smove(byte[] srckey, byte[] dstkey, byte[] member) {
        Long rtn;
        Object jedis = null;
        try {
            jedis = getResource();
            if (CLUSTER.equals(redisJedisPool.getPoolType())) {
                rtn = ((JedisCluster) jedis).smove(srckey, dstkey, member);
            } else {
                rtn = ((Jedis) jedis).smove(srckey, dstkey, member);
            }
            return rtn;
        } catch (Exception e) {
            throw e;
        } finally {
            returnResource(jedis);
        }
    }

    public Long sort(byte[] key, SortingParams sortingParameters, byte[] dstkey) {
        Long rtn;
        Object jedis = null;
        try {
            jedis = getResource();
            if (CLUSTER.equals(redisJedisPool.getPoolType())) {
                rtn = ((JedisCluster) jedis).sort(key, sortingParameters, dstkey);
            } else {
                rtn = ((Jedis) jedis).sort(key, sortingParameters, dstkey);
            }
            return rtn;
        } catch (Exception e) {
            throw e;
        } finally {
            returnResource(jedis);
        }
    }

    public Long sort(byte[] key, byte[] dstkey) {
        Long rtn;
        Object jedis = null;
        try {
            jedis = getResource();
            if (CLUSTER.equals(redisJedisPool.getPoolType())) {
                rtn = ((JedisCluster) jedis).sort(key, dstkey);
            } else {
                rtn = ((Jedis) jedis).sort(key, dstkey);
            }
            return rtn;
        } catch (Exception e) {
            throw e;
        } finally {
            returnResource(jedis);
        }
    }

    public Set<byte[]> sunion(byte[]... keys) {
        Set<byte[]> rtn;
        Object jedis = null;
        try {
            jedis = getResource();
            if (CLUSTER.equals(redisJedisPool.getPoolType())) {
                rtn = ((JedisCluster) jedis).sunion(keys);
            } else {
                rtn = ((Jedis) jedis).sunion(keys);
            }
            return rtn;
        } catch (Exception e) {
            throw e;
        } finally {
            returnResource(jedis);
        }
    }

    public Long sunionstore(byte[] dstkey, byte[]... keys) {
        Long rtn;
        Object jedis = null;
        try {
            jedis = getResource();
            if (CLUSTER.equals(redisJedisPool.getPoolType())) {
                rtn = ((JedisCluster) jedis).sunionstore(dstkey, keys);
            } else {
                rtn = ((Jedis) jedis).sunionstore(dstkey, keys);
            }
            return rtn;
        } catch (Exception e) {
            throw e;
        } finally {
            returnResource(jedis);
        }
    }

    public String watch(byte[]... keys) {
        String rtn;
        Object jedis = null;
        try {
            jedis = getResource();
            if (CLUSTER.equals(redisJedisPool.getPoolType())) {
                //                rtn = ((JedisCluster)jedis).watch(keys);
                throw new IllegalStateException("not support");
            } else {
                rtn = ((Jedis) jedis).watch(keys);
            }
            return rtn;
        } catch (Exception e) {
            throw e;
        } finally {
            returnResource(jedis);
        }
    }

    public String unwatch() {
        String rtn;
        Object jedis = null;
        try {
            jedis = getResource();
            if (CLUSTER.equals(redisJedisPool.getPoolType())) {
                //                rtn = ((JedisCluster)jedis).unwatch();
                throw new IllegalStateException("not support");
            } else {
                rtn = ((Jedis) jedis).unwatch();
            }
            return rtn;
        } catch (Exception e) {
            throw e;
        } finally {
            returnResource(jedis);
        }
    }

    public Long zinterstore(byte[] dstkey, byte[]... sets) {
        Long rtn;
        Object jedis = null;
        try {
            jedis = getResource();
            if (CLUSTER.equals(redisJedisPool.getPoolType())) {
                rtn = ((JedisCluster) jedis).zinterstore(dstkey, sets);
            } else {
                rtn = ((Jedis) jedis).zinterstore(dstkey, sets);
            }
            return rtn;
        } catch (Exception e) {
            throw e;
        } finally {
            returnResource(jedis);
        }
    }

    public Long zinterstore(byte[] dstkey, ZParams params, byte[]... sets) {
        Long rtn;
        Object jedis = null;
        try {
            jedis = getResource();
            if (CLUSTER.equals(redisJedisPool.getPoolType())) {
                rtn = ((JedisCluster) jedis).zinterstore(dstkey, params, sets);
            } else {
                rtn = ((Jedis) jedis).zinterstore(dstkey, params, sets);
            }
            return rtn;
        } catch (Exception e) {
            throw e;
        } finally {
            returnResource(jedis);
        }
    }

    public Long zunionstore(byte[] dstkey, byte[]... sets) {
        Long rtn;
        Object jedis = null;
        try {
            jedis = getResource();
            if (CLUSTER.equals(redisJedisPool.getPoolType())) {
                rtn = ((JedisCluster) jedis).zunionstore(dstkey, sets);
            } else {
                rtn = ((Jedis) jedis).zunionstore(dstkey, sets);
            }
            return rtn;
        } catch (Exception e) {
            throw e;
        } finally {
            returnResource(jedis);
        }
    }

    public Long zunionstore(byte[] dstkey, ZParams params, byte[]... sets) {
        Long rtn;
        Object jedis = null;
        try {
            jedis = getResource();
            if (CLUSTER.equals(redisJedisPool.getPoolType())) {
                rtn = ((JedisCluster) jedis).zunionstore(dstkey, params, sets);
            } else {
                rtn = ((Jedis) jedis).zunionstore(dstkey, params, sets);
            }
            return rtn;
        } catch (Exception e) {
            throw e;
        } finally {
            returnResource(jedis);
        }
    }

    public byte[] brpoplpush(byte[] source, byte[] destination, int timeout) {
        byte[] rtn;
        Object jedis = null;
        try {
            jedis = getResource();
            if (CLUSTER.equals(redisJedisPool.getPoolType())) {
                rtn = ((JedisCluster) jedis).brpoplpush(source, destination, timeout);
            } else {
                rtn = ((Jedis) jedis).brpoplpush(source, destination, timeout);
            }
            return rtn;
        } catch (Exception e) {
            throw e;
        } finally {
            returnResource(jedis);
        }
    }

    public Long publish(byte[] channel, byte[] message) {
        Long rtn;
        Object jedis = null;
        try {
            jedis = getResource();
            if (CLUSTER.equals(redisJedisPool.getPoolType())) {
                rtn = ((JedisCluster) jedis).publish(channel, message);
            } else {
                rtn = ((Jedis) jedis).publish(channel, message);
            }
            return rtn;
        } catch (Exception e) {
            throw e;
        } finally {
            returnResource(jedis);
        }
    }

    public void subscribe(BinaryJedisPubSub jedisPubSub, byte[]... channels) {
        Object jedis = null;
        try {
            jedis = getResource();
            if (CLUSTER.equals(redisJedisPool.getPoolType())) {
                ((JedisCluster) jedis).subscribe(jedisPubSub, channels);
            } else {
                ((Jedis) jedis).subscribe(jedisPubSub, channels);
            }
        } catch (Exception e) {
            throw e;
        } finally {
            returnResource(jedis);
        }
    }

    public void psubscribe(BinaryJedisPubSub jedisPubSub, byte[]... patterns) {
        Object jedis = null;
        try {
            jedis = getResource();
            if (CLUSTER.equals(redisJedisPool.getPoolType())) {
                ((JedisCluster) jedis).psubscribe(jedisPubSub, patterns);
            } else {
                ((Jedis) jedis).psubscribe(jedisPubSub, patterns);
            }
        } catch (Exception e) {
            throw e;
        } finally {
            returnResource(jedis);
        }
    }

    public byte[] randomBinaryKey() {
        byte[] rtn;
        Object jedis = null;
        try {
            jedis = getResource();
            if (CLUSTER.equals(redisJedisPool.getPoolType())) {
                //                rtn = ((JedisCluster)jedis).randomBinaryKey();
                throw new IllegalStateException("not support");
            } else {
                rtn = ((Jedis) jedis).randomBinaryKey();
            }
            return rtn;
        } catch (Exception e) {
            throw e;
        } finally {
            returnResource(jedis);
        }
    }

    public Long bitop(BitOP op, byte[] destKey, byte[]... srcKeys) {
        Long rtn;
        Object jedis = null;
        try {
            jedis = getResource();
            if (CLUSTER.equals(redisJedisPool.getPoolType())) {
                rtn = ((JedisCluster) jedis).bitop(op, destKey, srcKeys);
            } else {
                rtn = ((Jedis) jedis).bitop(op, destKey, srcKeys);
            }
            return rtn;
        } catch (Exception e) {
            throw e;
        } finally {
            returnResource(jedis);
        }
    }

    public String pfmerge(byte[] destkey, byte[]... sourcekeys) {
        String rtn;
        Object jedis = null;
        try {
            jedis = getResource();
            if (CLUSTER.equals(redisJedisPool.getPoolType())) {
                rtn = ((JedisCluster) jedis).pfmerge(destkey, sourcekeys);
            } else {
                rtn = ((Jedis) jedis).pfmerge(destkey, sourcekeys);
            }
            return rtn;
        } catch (Exception e) {
            throw e;
        } finally {
            returnResource(jedis);
        }
    }

    public Long pfcount(byte[]... keys) {
        Long rtn;
        Object jedis = null;
        try {
            jedis = getResource();
            if (CLUSTER.equals(redisJedisPool.getPoolType())) {
                rtn = ((JedisCluster) jedis).pfcount(keys);
            } else {
                rtn = ((Jedis) jedis).pfcount(keys);
            }
            return rtn;
        } catch (Exception e) {
            throw e;
        } finally {
            returnResource(jedis);
        }
    }

    public String set(byte[] key, byte[] value) {
        String rtn;
        Object jedis = null;
        try {
            jedis = getResource();
            if (CLUSTER.equals(redisJedisPool.getPoolType())) {
                rtn = ((JedisCluster) jedis).set(key, value);
            } else {
                rtn = ((Jedis) jedis).set(key, value);
            }
            return rtn;
        } catch (Exception e) {
            throw e;
        } finally {
            returnResource(jedis);
        }
    }

    public String set(byte[] bytes, byte[] bytes1, byte[] bytes2) {
        String rtn;
        Object jedis = null;
        try {
            jedis = getResource();
            if (CLUSTER.equals(redisJedisPool.getPoolType())) {
                rtn = ((JedisCluster) jedis).set(new String(bytes), new String(bytes1), new String(bytes2));
            } else {
                rtn = ((Jedis) jedis).set(bytes, bytes1, bytes2);
            }
            return rtn;
        } catch (Exception e) {
            throw e;
        } finally {
            returnResource(jedis);
        }
    }

    public String set(byte[] bytes, byte[] bytes1, byte[] bytes2, byte[] bytes3, long l) {
        String rtn;
        Object jedis = null;
        try {
            jedis = getResource();
            if (CLUSTER.equals(redisJedisPool.getPoolType())) {
                rtn = ((JedisCluster) jedis).set(bytes, bytes1, bytes2, bytes3, l);
            } else {
                rtn = ((Jedis) jedis).set(bytes, bytes1, bytes2, bytes3, l);
            }
            return rtn;
        } catch (Exception e) {
            throw e;
        } finally {
            returnResource(jedis);
        }
    }

    public byte[] get(byte[] key) {
        byte[] rtn;
        Object jedis = null;
        try {
            jedis = getResource();
            if (CLUSTER.equals(redisJedisPool.getPoolType())) {
                rtn = ((JedisCluster) jedis).get(key);
            } else {
                rtn = ((Jedis) jedis).get(key);
            }
            return rtn;
        } catch (Exception e) {
            throw e;
        } finally {
            returnResource(jedis);
        }
    }

    public Boolean exists(byte[] key) {
        Boolean rtn;
        Object jedis = null;
        try {
            jedis = getResource();
            if (CLUSTER.equals(redisJedisPool.getPoolType())) {
                rtn = ((JedisCluster) jedis).exists(key);
            } else {
                rtn = ((Jedis) jedis).exists(key);
            }
            return rtn;
        } catch (Exception e) {
            throw e;
        } finally {
            returnResource(jedis);
        }
    }

    public Long persist(byte[] key) {
        Long rtn;
        Object jedis = null;
        try {
            jedis = getResource();
            if (CLUSTER.equals(redisJedisPool.getPoolType())) {
                rtn = ((JedisCluster) jedis).persist(key);
            } else {
                rtn = ((Jedis) jedis).persist(key);
            }
            return rtn;
        } catch (Exception e) {
            throw e;
        } finally {
            returnResource(jedis);
        }
    }

    public String type(byte[] key) {
        String rtn;
        Object jedis = null;
        try {
            jedis = getResource();
            if (CLUSTER.equals(redisJedisPool.getPoolType())) {
                rtn = ((JedisCluster) jedis).type(key);
            } else {
                rtn = ((Jedis) jedis).type(key);
            }
            return rtn;
        } catch (Exception e) {
            throw e;
        } finally {
            returnResource(jedis);
        }
    }

    public Long expire(byte[] key, int seconds) {
        Long rtn;
        Object jedis = null;
        try {
            jedis = getResource();
            if (CLUSTER.equals(redisJedisPool.getPoolType())) {
                rtn = ((JedisCluster) jedis).expire(key, seconds);
            } else {
                rtn = ((Jedis) jedis).expire(key, seconds);
            }
            return rtn;
        } catch (Exception e) {
            throw e;
        } finally {
            returnResource(jedis);
        }
    }

    public Long pexpire(String key, long l) {
        Long rtn;
        Object jedis = null;
        try {
            jedis = getResource();
            if (CLUSTER.equals(redisJedisPool.getPoolType())) {
                rtn = ((JedisCluster) jedis).pexpire(key, l);
            } else {
                rtn = ((Jedis) jedis).pexpire(key, l);
            }
            return rtn;
        } catch (Exception e) {
            throw e;
        } finally {
            returnResource(jedis);
        }
    }

    public Long pexpire(byte[] bytes, long l) {
        Long rtn;
        Object jedis = null;
        try {
            jedis = getResource();
            if (CLUSTER.equals(redisJedisPool.getPoolType())) {
                rtn = ((JedisCluster) jedis).pexpire(bytes, l);
            } else {
                rtn = ((Jedis) jedis).pexpire(bytes, l);
            }
            return rtn;
        } catch (Exception e) {
            throw e;
        } finally {
            returnResource(jedis);
        }
    }

    public Long expireAt(byte[] key, long unixTime) {
        Long rtn;
        Object jedis = null;
        try {
            jedis = getResource();
            if (CLUSTER.equals(redisJedisPool.getPoolType())) {
                rtn = ((JedisCluster) jedis).expireAt(key, unixTime);
            } else {
                rtn = ((Jedis) jedis).expireAt(key, unixTime);
            }
            return rtn;
        } catch (Exception e) {
            throw e;
        } finally {
            returnResource(jedis);
        }
    }

    public Long pexpireAt(byte[] bytes, long l) {
        Long rtn;
        Object jedis = null;
        try {
            jedis = getResource();
            if (CLUSTER.equals(redisJedisPool.getPoolType())) {
                rtn = ((JedisCluster) jedis).pexpire(bytes, l);
            } else {
                rtn = ((Jedis) jedis).pexpire(bytes, l);
            }
            return rtn;
        } catch (Exception e) {
            throw e;
        } finally {
            returnResource(jedis);
        }
    }

    public Long ttl(byte[] key) {
        Long rtn;
        Object jedis = null;
        try {
            jedis = getResource();
            if (CLUSTER.equals(redisJedisPool.getPoolType())) {
                rtn = ((JedisCluster) jedis).ttl(key);
            } else {
                rtn = ((Jedis) jedis).ttl(key);
            }
            return rtn;
        } catch (Exception e) {
            throw e;
        } finally {
            returnResource(jedis);
        }
    }

    public Boolean setbit(byte[] key, long offset, boolean value) {
        Boolean rtn;
        Object jedis = null;
        try {
            jedis = getResource();
            if (CLUSTER.equals(redisJedisPool.getPoolType())) {
                rtn = ((JedisCluster) jedis).setbit(key, offset, value);
            } else {
                rtn = ((Jedis) jedis).setbit(key, offset, value);
            }
            return rtn;
        } catch (Exception e) {
            throw e;
        } finally {
            returnResource(jedis);
        }
    }

    public Boolean setbit(byte[] key, long offset, byte[] value) {
        Boolean rtn;
        Object jedis = null;
        try {
            jedis = getResource();
            if (CLUSTER.equals(redisJedisPool.getPoolType())) {
                rtn = ((JedisCluster) jedis).setbit(key, offset, value);
            } else {
                rtn = ((Jedis) jedis).setbit(key, offset, value);
            }
            return rtn;
        } catch (Exception e) {
            throw e;
        } finally {
            returnResource(jedis);
        }
    }

    public Boolean getbit(byte[] key, long offset) {
        Boolean rtn;
        Object jedis = null;
        try {
            jedis = getResource();
            if (CLUSTER.equals(redisJedisPool.getPoolType())) {
                rtn = ((JedisCluster) jedis).getbit(key, offset);
            } else {
                rtn = ((Jedis) jedis).getbit(key, offset);
            }
            return rtn;
        } catch (Exception e) {
            throw e;
        } finally {
            returnResource(jedis);
        }
    }

    public Long setrange(byte[] key, long offset, byte[] value) {
        Long rtn;
        Object jedis = null;
        try {
            jedis = getResource();
            if (CLUSTER.equals(redisJedisPool.getPoolType())) {
                rtn = ((JedisCluster) jedis).setrange(key, offset, value);
            } else {
                rtn = ((Jedis) jedis).setrange(key, offset, value);
            }
            return rtn;
        } catch (Exception e) {
            throw e;
        } finally {
            returnResource(jedis);
        }
    }

    public byte[] getrange(byte[] key, long startOffset, long endOffset) {
        byte[] rtn;
        Object jedis = null;
        try {
            jedis = getResource();
            if (CLUSTER.equals(redisJedisPool.getPoolType())) {
                rtn = ((JedisCluster) jedis).getrange(key, startOffset, endOffset);
            } else {
                rtn = ((Jedis) jedis).getrange(key, startOffset, endOffset);
            }
            return rtn;
        } catch (Exception e) {
            throw e;
        } finally {
            returnResource(jedis);
        }
    }

    public byte[] getSet(byte[] key, byte[] value) {
        byte[] rtn;
        Object jedis = null;
        try {
            jedis = getResource();
            if (CLUSTER.equals(redisJedisPool.getPoolType())) {
                rtn = ((JedisCluster) jedis).getSet(key, value);
            } else {
                rtn = ((Jedis) jedis).getSet(key, value);
            }
            return rtn;
        } catch (Exception e) {
            throw e;
        } finally {
            returnResource(jedis);
        }
    }

    public Long setnx(byte[] key, byte[] value) {
        Long rtn;
        Object jedis = null;
        try {
            jedis = getResource();
            if (CLUSTER.equals(redisJedisPool.getPoolType())) {
                rtn = ((JedisCluster) jedis).setnx(key, value);
            } else {
                rtn = ((Jedis) jedis).setnx(key, value);
            }
            return rtn;
        } catch (Exception e) {
            throw e;
        } finally {
            returnResource(jedis);
        }
    }

    public String setex(byte[] key, int seconds, byte[] value) {
        String rtn;
        Object jedis = null;
        try {
            jedis = getResource();
            if (CLUSTER.equals(redisJedisPool.getPoolType())) {
                rtn = ((JedisCluster) jedis).setex(key, seconds, value);
            } else {
                rtn = ((Jedis) jedis).setex(key, seconds, value);
            }
            return rtn;
        } catch (Exception e) {
            throw e;
        } finally {
            returnResource(jedis);
        }
    }

    public Long decrBy(byte[] key, long integer) {
        Long rtn;
        Object jedis = null;
        try {
            jedis = getResource();
            if (CLUSTER.equals(redisJedisPool.getPoolType())) {
                rtn = ((JedisCluster) jedis).decrBy(key, integer);
            } else {
                rtn = ((Jedis) jedis).decrBy(key, integer);
            }
            return rtn;
        } catch (Exception e) {
            throw e;
        } finally {
            returnResource(jedis);
        }
    }

    public Long decr(byte[] key) {
        Long rtn;
        Object jedis = null;
        try {
            jedis = getResource();
            if (CLUSTER.equals(redisJedisPool.getPoolType())) {
                rtn = ((JedisCluster) jedis).decr(key);
            } else {
                rtn = ((Jedis) jedis).decr(key);
            }
            return rtn;
        } catch (Exception e) {
            throw e;
        } finally {
            returnResource(jedis);
        }
    }

    public Long incrBy(byte[] key, long integer) {
        Long rtn;
        Object jedis = null;
        try {
            jedis = getResource();
            if (CLUSTER.equals(redisJedisPool.getPoolType())) {
                rtn = ((JedisCluster) jedis).incrBy(key, integer);
            } else {
                rtn = ((Jedis) jedis).incrBy(key, integer);
            }
            return rtn;
        } catch (Exception e) {
            throw e;
        } finally {
            returnResource(jedis);
        }
    }

    public Double incrByFloat(byte[] key, double value) {
        Double rtn;
        Object jedis = null;
        try {
            jedis = getResource();
            if (CLUSTER.equals(redisJedisPool.getPoolType())) {
                rtn = ((JedisCluster) jedis).incrByFloat(key, value);
            } else {
                rtn = ((Jedis) jedis).incrByFloat(key, value);
            }
            return rtn;
        } catch (Exception e) {
            throw e;
        } finally {
            returnResource(jedis);
        }
    }

    public Long incr(byte[] key) {
        Long rtn;
        Object jedis = null;
        try {
            jedis = getResource();
            if (CLUSTER.equals(redisJedisPool.getPoolType())) {
                rtn = ((JedisCluster) jedis).incr(key);
            } else {
                rtn = ((Jedis) jedis).incr(key);
            }
            return rtn;
        } catch (Exception e) {
            throw e;
        } finally {
            returnResource(jedis);
        }
    }

    public Long append(byte[] key, byte[] value) {
        Long rtn;
        Object jedis = null;
        try {
            jedis = getResource();
            if (CLUSTER.equals(redisJedisPool.getPoolType())) {
                rtn = ((JedisCluster) jedis).append(key, value);
            } else {
                rtn = ((Jedis) jedis).append(key, value);
            }
            return rtn;
        } catch (Exception e) {
            throw e;
        } finally {
            returnResource(jedis);
        }
    }

    public byte[] substr(byte[] key, int start, int end) {
        byte[] rtn;
        Object jedis = null;
        try {
            jedis = getResource();
            if (CLUSTER.equals(redisJedisPool.getPoolType())) {
                rtn = ((JedisCluster) jedis).substr(key, start, end);
            } else {
                rtn = ((Jedis) jedis).substr(key, start, end);
            }
            return rtn;
        } catch (Exception e) {
            throw e;
        } finally {
            returnResource(jedis);
        }
    }

    public Long hset(byte[] key, byte[] field, byte[] value) {
        Long rtn;
        Object jedis = null;
        try {
            jedis = getResource();
            if (CLUSTER.equals(redisJedisPool.getPoolType())) {
                rtn = ((JedisCluster) jedis).hset(key, field, value);
            } else {
                rtn = ((Jedis) jedis).hset(key, field, value);
            }
            return rtn;
        } catch (Exception e) {
            throw e;
        } finally {
            returnResource(jedis);
        }
    }

    public byte[] hget(byte[] key, byte[] field) {
        byte[] rtn;
        Object jedis = null;
        try {
            jedis = getResource();
            if (CLUSTER.equals(redisJedisPool.getPoolType())) {
                rtn = ((JedisCluster) jedis).hget(key, field);
            } else {
                rtn = ((Jedis) jedis).hget(key, field);
            }
            return rtn;
        } catch (Exception e) {
            throw e;
        } finally {
            returnResource(jedis);
        }
    }

    public Long hsetnx(byte[] key, byte[] field, byte[] value) {
        Long rtn;
        Object jedis = null;
        try {
            jedis = getResource();
            if (CLUSTER.equals(redisJedisPool.getPoolType())) {
                rtn = ((JedisCluster) jedis).hsetnx(key, field, value);
            } else {
                rtn = ((Jedis) jedis).hsetnx(key, field, value);
            }
            return rtn;
        } catch (Exception e) {
            throw e;
        } finally {
            returnResource(jedis);
        }
    }

    public String hmset(byte[] key, Map<byte[], byte[]> hash) {
        String rtn;
        Object jedis = null;
        try {
            jedis = getResource();
            if (CLUSTER.equals(redisJedisPool.getPoolType())) {
                rtn = ((JedisCluster) jedis).hmset(key, hash);
            } else {
                rtn = ((Jedis) jedis).hmset(key, hash);
            }
            return rtn;
        } catch (Exception e) {
            throw e;
        } finally {
            returnResource(jedis);
        }
    }

    public List<byte[]> hmget(byte[] key, byte[]... fields) {
        List<byte[]> rtn;
        Object jedis = null;
        try {
            jedis = getResource();
            if (CLUSTER.equals(redisJedisPool.getPoolType())) {
                rtn = ((JedisCluster) jedis).hmget(key, fields);
            } else {
                rtn = ((Jedis) jedis).hmget(key, fields);
            }
            return rtn;
        } catch (Exception e) {
            throw e;
        } finally {
            returnResource(jedis);
        }
    }

    public Long hincrBy(byte[] key, byte[] field, long value) {
        Long rtn;
        Object jedis = null;
        try {
            jedis = getResource();
            if (CLUSTER.equals(redisJedisPool.getPoolType())) {
                rtn = ((JedisCluster) jedis).hincrBy(key, field, value);
            } else {
                rtn = ((Jedis) jedis).hincrBy(key, field, value);
            }
            return rtn;
        } catch (Exception e) {
            throw e;
        } finally {
            returnResource(jedis);
        }
    }

    public Double hincrByFloat(byte[] key, byte[] field, double value) {
        Double rtn;
        Object jedis = null;
        try {
            jedis = getResource();
            if (CLUSTER.equals(redisJedisPool.getPoolType())) {
                rtn = ((JedisCluster) jedis).hincrByFloat(key, field, value);
            } else {
                rtn = ((Jedis) jedis).hincrByFloat(key, field, value);
            }
            return rtn;
        } catch (Exception e) {
            throw e;
        } finally {
            returnResource(jedis);
        }
    }

    public Boolean hexists(byte[] key, byte[] field) {
        Boolean rtn;
        Object jedis = null;
        try {
            jedis = getResource();
            if (CLUSTER.equals(redisJedisPool.getPoolType())) {
                rtn = ((JedisCluster) jedis).hexists(key, field);
            } else {
                rtn = ((Jedis) jedis).hexists(key, field);
            }
            return rtn;
        } catch (Exception e) {
            throw e;
        } finally {
            returnResource(jedis);
        }
    }

    public Long hdel(byte[] key, byte[]... fields) {
        Long rtn;
        Object jedis = null;
        try {
            jedis = getResource();
            if (CLUSTER.equals(redisJedisPool.getPoolType())) {
                rtn = ((JedisCluster) jedis).hdel(key, fields);
            } else {
                rtn = ((Jedis) jedis).hdel(key, fields);
            }
            return rtn;
        } catch (Exception e) {
            throw e;
        } finally {
            returnResource(jedis);
        }
    }

    public Long hlen(byte[] key) {
        Long rtn;
        Object jedis = null;
        try {
            jedis = getResource();
            if (CLUSTER.equals(redisJedisPool.getPoolType())) {
                rtn = ((JedisCluster) jedis).hlen(key);
            } else {
                rtn = ((Jedis) jedis).hlen(key);
            }
            return rtn;
        } catch (Exception e) {
            throw e;
        } finally {
            returnResource(jedis);
        }
    }

    public Set<byte[]> hkeys(byte[] key) {
        Set<byte[]> rtn;
        Object jedis = null;
        try {
            jedis = getResource();
            if (CLUSTER.equals(redisJedisPool.getPoolType())) {
                rtn = ((JedisCluster) jedis).hkeys(key);
            } else {
                rtn = ((Jedis) jedis).hkeys(key);
            }
            return rtn;
        } catch (Exception e) {
            throw e;
        } finally {
            returnResource(jedis);
        }
    }

    public Collection<byte[]> hvals(byte[] key) {
        Collection<byte[]> rtn;
        Object jedis = null;
        try {
            jedis = getResource();
            if (CLUSTER.equals(redisJedisPool.getPoolType())) {
                rtn = ((JedisCluster) jedis).hvals(key);
            } else {
                rtn = ((Jedis) jedis).hvals(key);
            }
            return rtn;
        } catch (Exception e) {
            throw e;
        } finally {
            returnResource(jedis);
        }
    }

    public Map<byte[], byte[]> hgetAll(byte[] key) {
        Map<byte[], byte[]> rtn;
        Object jedis = null;
        try {
            jedis = getResource();
            if (CLUSTER.equals(redisJedisPool.getPoolType())) {
                rtn = ((JedisCluster) jedis).hgetAll(key);
            } else {
                rtn = ((Jedis) jedis).hgetAll(key);
            }
            return rtn;
        } catch (Exception e) {
            throw e;
        } finally {
            returnResource(jedis);
        }
    }

    public Long rpush(byte[] key, byte[]... args) {
        Long rtn;
        Object jedis = null;
        try {
            jedis = getResource();
            if (CLUSTER.equals(redisJedisPool.getPoolType())) {
                rtn = ((JedisCluster) jedis).rpush(key, args);
            } else {
                rtn = ((Jedis) jedis).rpush(key, args);
            }
            return rtn;
        } catch (Exception e) {
            throw e;
        } finally {
            returnResource(jedis);
        }
    }

    public Long lpush(byte[] key, byte[]... args) {
        Long rtn;
        Object jedis = null;
        try {
            jedis = getResource();
            if (CLUSTER.equals(redisJedisPool.getPoolType())) {
                rtn = ((JedisCluster) jedis).lpush(key, args);
            } else {
                rtn = ((Jedis) jedis).lpush(key, args);
            }
            return rtn;
        } catch (Exception e) {
            throw e;
        } finally {
            returnResource(jedis);
        }
    }

    public Long llen(byte[] key) {
        Long rtn;
        Object jedis = null;
        try {
            jedis = getResource();
            if (CLUSTER.equals(redisJedisPool.getPoolType())) {
                rtn = ((JedisCluster) jedis).llen(key);
            } else {
                rtn = ((Jedis) jedis).llen(key);
            }
            return rtn;
        } catch (Exception e) {
            throw e;
        } finally {
            returnResource(jedis);
        }
    }

    public List<byte[]> lrange(byte[] key, long start, long end) {
        List<byte[]> rtn;
        Object jedis = null;
        try {
            jedis = getResource();
            if (CLUSTER.equals(redisJedisPool.getPoolType())) {
                rtn = ((JedisCluster) jedis).lrange(key, start, end);
            } else {
                rtn = ((Jedis) jedis).lrange(key, start, end);
            }
            return rtn;
        } catch (Exception e) {
            throw e;
        } finally {
            returnResource(jedis);
        }
    }

    public String ltrim(byte[] key, long start, long end) {
        String rtn;
        Object jedis = null;
        try {
            jedis = getResource();
            if (CLUSTER.equals(redisJedisPool.getPoolType())) {
                rtn = ((JedisCluster) jedis).ltrim(key, start, end);
            } else {
                rtn = ((Jedis) jedis).ltrim(key, start, end);
            }
            return rtn;
        } catch (Exception e) {
            throw e;
        } finally {
            returnResource(jedis);
        }
    }

    public byte[] lindex(byte[] key, long index) {
        byte[] rtn;
        Object jedis = null;
        try {
            jedis = getResource();
            if (CLUSTER.equals(redisJedisPool.getPoolType())) {
                rtn = ((JedisCluster) jedis).lindex(key, index);
            } else {
                rtn = ((Jedis) jedis).lindex(key, index);
            }
            return rtn;
        } catch (Exception e) {
            throw e;
        } finally {
            returnResource(jedis);
        }
    }

    public String lset(byte[] key, long index, byte[] value) {
        String rtn;
        Object jedis = null;
        try {
            jedis = getResource();
            if (CLUSTER.equals(redisJedisPool.getPoolType())) {
                rtn = ((JedisCluster) jedis).lset(key, index, value);
            } else {
                rtn = ((Jedis) jedis).lset(key, index, value);
            }
            return rtn;
        } catch (Exception e) {
            throw e;
        } finally {
            returnResource(jedis);
        }
    }

    public Long lrem(byte[] key, long count, byte[] value) {
        Long rtn;
        Object jedis = null;
        try {
            jedis = getResource();
            if (CLUSTER.equals(redisJedisPool.getPoolType())) {
                rtn = ((JedisCluster) jedis).lrem(key, count, value);
            } else {
                rtn = ((Jedis) jedis).lrem(key, count, value);
            }
            return rtn;
        } catch (Exception e) {
            throw e;
        } finally {
            returnResource(jedis);
        }
    }

    public byte[] lpop(byte[] key) {
        byte[] rtn;
        Object jedis = null;
        try {
            jedis = getResource();
            if (CLUSTER.equals(redisJedisPool.getPoolType())) {
                rtn = ((JedisCluster) jedis).lpop(key);
            } else {
                rtn = ((Jedis) jedis).lpop(key);
            }
            return rtn;
        } catch (Exception e) {
            throw e;
        } finally {
            returnResource(jedis);
        }
    }

    public byte[] rpop(byte[] key) {
        byte[] rtn;
        Object jedis = null;
        try {
            jedis = getResource();
            if (CLUSTER.equals(redisJedisPool.getPoolType())) {
                rtn = ((JedisCluster) jedis).rpop(key);
            } else {
                rtn = ((Jedis) jedis).rpop(key);
            }
            return rtn;
        } catch (Exception e) {
            throw e;
        } finally {
            returnResource(jedis);
        }
    }

    public Long sadd(byte[] key, byte[]... members) {
        Long rtn;
        Object jedis = null;
        try {
            jedis = getResource();
            if (CLUSTER.equals(redisJedisPool.getPoolType())) {
                rtn = ((JedisCluster) jedis).sadd(key, members);
            } else {
                rtn = ((Jedis) jedis).sadd(key, members);
            }
            return rtn;
        } catch (Exception e) {
            throw e;
        } finally {
            returnResource(jedis);
        }
    }

    public Set<byte[]> smembers(byte[] key) {
        Set<byte[]> rtn;
        Object jedis = null;
        try {
            jedis = getResource();
            if (CLUSTER.equals(redisJedisPool.getPoolType())) {
                rtn = ((JedisCluster) jedis).smembers(key);
            } else {
                rtn = ((Jedis) jedis).smembers(key);
            }
            return rtn;
        } catch (Exception e) {
            throw e;
        } finally {
            returnResource(jedis);
        }
    }

    public Long srem(byte[] key, byte[]... member) {
        Long rtn;
        Object jedis = null;
        try {
            jedis = getResource();
            if (CLUSTER.equals(redisJedisPool.getPoolType())) {
                rtn = ((JedisCluster) jedis).srem(key, member);
            } else {
                rtn = ((Jedis) jedis).srem(key, member);
            }
            return rtn;
        } catch (Exception e) {
            throw e;
        } finally {
            returnResource(jedis);
        }
    }

    public byte[] spop(byte[] key) {
        byte[] rtn;
        Object jedis = null;
        try {
            jedis = getResource();
            if (CLUSTER.equals(redisJedisPool.getPoolType())) {
                rtn = ((JedisCluster) jedis).spop(key);
            } else {
                rtn = ((Jedis) jedis).spop(key);
            }
            return rtn;
        } catch (Exception e) {
            throw e;
        } finally {
            returnResource(jedis);
        }
    }

    public Set<byte[]> spop(byte[] bytes, long l) {
        Set<byte[]> rtn;
        Object jedis = null;
        try {
            jedis = getResource();
            if (CLUSTER.equals(redisJedisPool.getPoolType())) {
                rtn = ((JedisCluster) jedis).spop(bytes, l);
            } else {
                rtn = ((Jedis) jedis).spop(bytes, l);
            }
            return rtn;
        } catch (Exception e) {
            throw e;
        } finally {
            returnResource(jedis);
        }
    }

    public Long scard(byte[] key) {
        Long rtn;
        Object jedis = null;
        try {
            jedis = getResource();
            if (CLUSTER.equals(redisJedisPool.getPoolType())) {
                rtn = ((JedisCluster) jedis).scard(key);
            } else {
                rtn = ((Jedis) jedis).scard(key);
            }
            return rtn;
        } catch (Exception e) {
            throw e;
        } finally {
            returnResource(jedis);
        }
    }

    public Boolean sismember(byte[] key, byte[] member) {
        Boolean rtn;
        Object jedis = null;
        try {
            jedis = getResource();
            if (CLUSTER.equals(redisJedisPool.getPoolType())) {
                rtn = ((JedisCluster) jedis).sismember(key, member);
            } else {
                rtn = ((Jedis) jedis).sismember(key, member);
            }
            return rtn;
        } catch (Exception e) {
            throw e;
        } finally {
            returnResource(jedis);
        }
    }

    public byte[] srandmember(byte[] key) {
        byte[] rtn;
        Object jedis = null;
        try {
            jedis = getResource();
            if (CLUSTER.equals(redisJedisPool.getPoolType())) {
                rtn = ((JedisCluster) jedis).srandmember(key);
            } else {
                rtn = ((Jedis) jedis).srandmember(key);
            }
            return rtn;
        } catch (Exception e) {
            throw e;
        } finally {
            returnResource(jedis);
        }
    }

    public List<byte[]> srandmember(byte[] key, int count) {
        List<byte[]> rtn;
        Object jedis = null;
        try {
            jedis = getResource();
            if (CLUSTER.equals(redisJedisPool.getPoolType())) {
                rtn = ((JedisCluster) jedis).srandmember(key, count);
            } else {
                rtn = ((Jedis) jedis).srandmember(key, count);
            }
            return rtn;
        } catch (Exception e) {
            throw e;
        } finally {
            returnResource(jedis);
        }
    }

    public Long strlen(byte[] key) {
        Long rtn;
        Object jedis = null;
        try {
            jedis = getResource();
            if (CLUSTER.equals(redisJedisPool.getPoolType())) {
                rtn = ((JedisCluster) jedis).strlen(key);
            } else {
                rtn = ((Jedis) jedis).strlen(key);
            }
            return rtn;
        } catch (Exception e) {
            throw e;
        } finally {
            returnResource(jedis);
        }
    }

    public Long zadd(byte[] key, double score, byte[] member) {
        Long rtn;
        Object jedis = null;
        try {
            jedis = getResource();
            if (CLUSTER.equals(redisJedisPool.getPoolType())) {
                rtn = ((JedisCluster) jedis).zadd(key, score, member);
            } else {
                rtn = ((Jedis) jedis).zadd(key, score, member);
            }
            return rtn;
        } catch (Exception e) {
            throw e;
        } finally {
            returnResource(jedis);
        }
    }

    public Long zadd(byte[] key, Map<byte[], Double> scoreMembers) {
        Long rtn;
        Object jedis = null;
        try {
            jedis = getResource();
            if (CLUSTER.equals(redisJedisPool.getPoolType())) {
                rtn = ((JedisCluster) jedis).zadd(key, scoreMembers);
            } else {
                rtn = ((Jedis) jedis).zadd(key, scoreMembers);
            }
            return rtn;
        } catch (Exception e) {
            throw e;
        } finally {
            returnResource(jedis);
        }
    }

    public Set<byte[]> zrange(byte[] key, long start, long end) {
        Set<byte[]> rtn;
        Object jedis = null;
        try {
            jedis = getResource();
            if (CLUSTER.equals(redisJedisPool.getPoolType())) {
                rtn = ((JedisCluster) jedis).zrange(key, start, end);
            } else {
                rtn = ((Jedis) jedis).zrange(key, start, end);
            }
            return rtn;
        } catch (Exception e) {
            throw e;
        } finally {
            returnResource(jedis);
        }
    }

    public Long zrem(byte[] key, byte[]... members) {
        Long rtn;
        Object jedis = null;
        try {
            jedis = getResource();
            if (CLUSTER.equals(redisJedisPool.getPoolType())) {
                rtn = ((JedisCluster) jedis).zrem(key, members);
            } else {
                rtn = ((Jedis) jedis).zrem(key, members);
            }
            return rtn;
        } catch (Exception e) {
            throw e;
        } finally {
            returnResource(jedis);
        }
    }

    public Double zincrby(byte[] key, double score, byte[] member) {
        Double rtn;
        Object jedis = null;
        try {
            jedis = getResource();
            if (CLUSTER.equals(redisJedisPool.getPoolType())) {
                rtn = ((JedisCluster) jedis).zincrby(key, score, member);
            } else {
                rtn = ((Jedis) jedis).zincrby(key, score, member);
            }
            return rtn;
        } catch (Exception e) {
            throw e;
        } finally {
            returnResource(jedis);
        }
    }

    public Long zrank(byte[] key, byte[] member) {
        Long rtn;
        Object jedis = null;
        try {
            jedis = getResource();
            if (CLUSTER.equals(redisJedisPool.getPoolType())) {
                rtn = ((JedisCluster) jedis).zrank(key, member);
            } else {
                rtn = ((Jedis) jedis).zrank(key, member);
            }
            return rtn;
        } catch (Exception e) {
            throw e;
        } finally {
            returnResource(jedis);
        }
    }

    public Long zrevrank(byte[] key, byte[] member) {
        Long rtn;
        Object jedis = null;
        try {
            jedis = getResource();
            if (CLUSTER.equals(redisJedisPool.getPoolType())) {
                rtn = ((JedisCluster) jedis).zrevrank(key, member);
            } else {
                rtn = ((Jedis) jedis).zrevrank(key, member);
            }
            return rtn;
        } catch (Exception e) {
            throw e;
        } finally {
            returnResource(jedis);
        }
    }

    public Set<byte[]> zrevrange(byte[] key, long start, long end) {
        Set<byte[]> rtn;
        Object jedis = null;
        try {
            jedis = getResource();
            if (CLUSTER.equals(redisJedisPool.getPoolType())) {
                rtn = ((JedisCluster) jedis).zrevrange(key, start, end);
            } else {
                rtn = ((Jedis) jedis).zrevrange(key, start, end);
            }
            return rtn;
        } catch (Exception e) {
            throw e;
        } finally {
            returnResource(jedis);
        }
    }

    public Set<Tuple> zrangeWithScores(byte[] key, long start, long end) {
        Set<Tuple> rtn;
        Object jedis = null;
        try {
            jedis = getResource();
            if (CLUSTER.equals(redisJedisPool.getPoolType())) {
                rtn = ((JedisCluster) jedis).zrangeWithScores(key, start, end);
            } else {
                rtn = ((Jedis) jedis).zrangeWithScores(key, start, end);
            }
            return rtn;
        } catch (Exception e) {
            throw e;
        } finally {
            returnResource(jedis);
        }
    }

    public Set<Tuple> zrevrangeWithScores(byte[] key, long start, long end) {
        Set<Tuple> rtn;
        Object jedis = null;
        try {
            jedis = getResource();
            if (CLUSTER.equals(redisJedisPool.getPoolType())) {
                rtn = ((JedisCluster) jedis).zrevrangeWithScores(key, start, end);
            } else {
                rtn = ((Jedis) jedis).zrevrangeWithScores(key, start, end);
            }
            return rtn;
        } catch (Exception e) {
            throw e;
        } finally {
            returnResource(jedis);
        }
    }

    public Long zcard(byte[] key) {
        Long rtn;
        Object jedis = null;
        try {
            jedis = getResource();
            if (CLUSTER.equals(redisJedisPool.getPoolType())) {
                rtn = ((JedisCluster) jedis).zcard(key);
            } else {
                rtn = ((Jedis) jedis).zcard(key);
            }
            return rtn;
        } catch (Exception e) {
            throw e;
        } finally {
            returnResource(jedis);
        }
    }

    public Double zscore(byte[] key, byte[] member) {
        Double rtn;
        Object jedis = null;
        try {
            jedis = getResource();
            if (CLUSTER.equals(redisJedisPool.getPoolType())) {
                rtn = ((JedisCluster) jedis).zscore(key, member);
            } else {
                rtn = ((Jedis) jedis).zscore(key, member);
            }
            return rtn;
        } catch (Exception e) {
            throw e;
        } finally {
            returnResource(jedis);
        }
    }

    public List<byte[]> sort(byte[] key) {
        List<byte[]> rtn;
        Object jedis = null;
        try {
            jedis = getResource();
            if (CLUSTER.equals(redisJedisPool.getPoolType())) {
                rtn = ((JedisCluster) jedis).sort(key);
            } else {
                rtn = ((Jedis) jedis).sort(key);
            }
            return rtn;
        } catch (Exception e) {
            throw e;
        } finally {
            returnResource(jedis);
        }
    }

    public List<byte[]> sort(byte[] key, SortingParams sortingParameters) {
        List<byte[]> rtn;
        Object jedis = null;
        try {
            jedis = getResource();
            if (CLUSTER.equals(redisJedisPool.getPoolType())) {
                rtn = ((JedisCluster) jedis).sort(key, sortingParameters);
            } else {
                rtn = ((Jedis) jedis).sort(key, sortingParameters);
            }
            return rtn;
        } catch (Exception e) {
            throw e;
        } finally {
            returnResource(jedis);
        }
    }

    public Long zcount(byte[] key, double min, double max) {
        Long rtn;
        Object jedis = null;
        try {
            jedis = getResource();
            if (CLUSTER.equals(redisJedisPool.getPoolType())) {
                rtn = ((JedisCluster) jedis).zcount(key, min, max);
            } else {
                rtn = ((Jedis) jedis).zcount(key, min, max);
            }
            return rtn;
        } catch (Exception e) {
            throw e;
        } finally {
            returnResource(jedis);
        }
    }

    public Long zcount(byte[] key, byte[] min, byte[] max) {
        Long rtn;
        Object jedis = null;
        try {
            jedis = getResource();
            if (CLUSTER.equals(redisJedisPool.getPoolType())) {
                rtn = ((JedisCluster) jedis).zcount(key, min, max);
            } else {
                rtn = ((Jedis) jedis).zcount(key, min, max);
            }
            return rtn;
        } catch (Exception e) {
            throw e;
        } finally {
            returnResource(jedis);
        }
    }

    public Set<byte[]> zrangeByScore(byte[] key, double min, double max) {
        Set<byte[]> rtn;
        Object jedis = null;
        try {
            jedis = getResource();
            if (CLUSTER.equals(redisJedisPool.getPoolType())) {
                rtn = ((JedisCluster) jedis).zrangeByScore(key, min, max);
            } else {
                rtn = ((Jedis) jedis).zrangeByScore(key, min, max);
            }
            return rtn;
        } catch (Exception e) {
            throw e;
        } finally {
            returnResource(jedis);
        }
    }

    public Set<byte[]> zrangeByScore(byte[] key, byte[] min, byte[] max) {
        Set<byte[]> rtn;
        Object jedis = null;
        try {
            jedis = getResource();
            if (CLUSTER.equals(redisJedisPool.getPoolType())) {
                rtn = ((JedisCluster) jedis).zrangeByScore(key, min, max);
            } else {
                rtn = ((Jedis) jedis).zrangeByScore(key, min, max);
            }
            return rtn;
        } catch (Exception e) {
            throw e;
        } finally {
            returnResource(jedis);
        }
    }

    public Set<byte[]> zrevrangeByScore(byte[] key, double max, double min) {
        Set<byte[]> rtn;
        Object jedis = null;
        try {
            jedis = getResource();
            if (CLUSTER.equals(redisJedisPool.getPoolType())) {
                rtn = ((JedisCluster) jedis).zrevrangeByScore(key, max, min);
            } else {
                rtn = ((Jedis) jedis).zrevrangeByScore(key, max, min);
            }
            return rtn;
        } catch (Exception e) {
            throw e;
        } finally {
            returnResource(jedis);
        }
    }

    public Set<byte[]> zrevrangeByScore(byte[] key, byte[] max, byte[] min) {
        Set<byte[]> rtn;
        Object jedis = null;
        try {
            jedis = getResource();
            if (CLUSTER.equals(redisJedisPool.getPoolType())) {
                rtn = ((JedisCluster) jedis).zrevrangeByScore(key, max, min);
            } else {
                rtn = ((Jedis) jedis).zrevrangeByScore(key, max, min);
            }
            return rtn;
        } catch (Exception e) {
            throw e;
        } finally {
            returnResource(jedis);
        }
    }

    public Set<Tuple> zrangeByScoreWithScores(byte[] key, double min, double max) {
        Set<Tuple> rtn;
        Object jedis = null;
        try {
            jedis = getResource();
            if (CLUSTER.equals(redisJedisPool.getPoolType())) {
                rtn = ((JedisCluster) jedis).zrangeByScoreWithScores(key, min, max);
            } else {
                rtn = ((Jedis) jedis).zrangeByScoreWithScores(key, min, max);
            }
            return rtn;
        } catch (Exception e) {
            throw e;
        } finally {
            returnResource(jedis);
        }
    }

    public Set<Tuple> zrangeByScoreWithScores(byte[] key, byte[] min, byte[] max) {
        Set<Tuple> rtn;
        Object jedis = null;
        try {
            jedis = getResource();
            if (CLUSTER.equals(redisJedisPool.getPoolType())) {
                rtn = ((JedisCluster) jedis).zrangeByScoreWithScores(key, min, max);
            } else {
                rtn = ((Jedis) jedis).zrangeByScoreWithScores(key, min, max);
            }
            return rtn;
        } catch (Exception e) {
            throw e;
        } finally {
            returnResource(jedis);
        }
    }

    public Long zremrangeByRank(byte[] key, long start, long end) {
        Long rtn;
        Object jedis = null;
        try {
            jedis = getResource();
            if (CLUSTER.equals(redisJedisPool.getPoolType())) {
                rtn = ((JedisCluster) jedis).zremrangeByRank(key, start, end);
            } else {
                rtn = ((Jedis) jedis).zremrangeByRank(key, start, end);
            }
            return rtn;
        } catch (Exception e) {
            throw e;
        } finally {
            returnResource(jedis);
        }
    }

    public Long zremrangeByScore(byte[] key, double start, double end) {
        Long rtn;
        Object jedis = null;
        try {
            jedis = getResource();
            if (CLUSTER.equals(redisJedisPool.getPoolType())) {
                rtn = ((JedisCluster) jedis).zremrangeByScore(key, start, end);
            } else {
                rtn = ((Jedis) jedis).zremrangeByScore(key, start, end);
            }
            return rtn;
        } catch (Exception e) {
            throw e;
        } finally {
            returnResource(jedis);
        }
    }

    public Long zremrangeByScore(byte[] key, byte[] start, byte[] end) {
        Long rtn;
        Object jedis = null;
        try {
            jedis = getResource();
            if (CLUSTER.equals(redisJedisPool.getPoolType())) {
                rtn = ((JedisCluster) jedis).zremrangeByScore(key, start, end);
            } else {
                rtn = ((Jedis) jedis).zremrangeByScore(key, start, end);
            }
            return rtn;
        } catch (Exception e) {
            throw e;
        } finally {
            returnResource(jedis);
        }
    }

    public Long zlexcount(byte[] key, byte[] min, byte[] max) {
        Long rtn;
        Object jedis = null;
        try {
            jedis = getResource();
            if (CLUSTER.equals(redisJedisPool.getPoolType())) {
                rtn = ((JedisCluster) jedis).zlexcount(key, min, max);
            } else {
                rtn = ((Jedis) jedis).zlexcount(key, min, max);
            }
            return rtn;
        } catch (Exception e) {
            throw e;
        } finally {
            returnResource(jedis);
        }
    }

    public Set<byte[]> zrangeByLex(byte[] key, byte[] min, byte[] max) {
        Set<byte[]> rtn;
        Object jedis = null;
        try {
            jedis = getResource();
            if (CLUSTER.equals(redisJedisPool.getPoolType())) {
                rtn = ((JedisCluster) jedis).zrangeByLex(key, min, max);
            } else {
                rtn = ((Jedis) jedis).zrangeByLex(key, min, max);
            }
            return rtn;
        } catch (Exception e) {
            throw e;
        } finally {
            returnResource(jedis);
        }
    }

    public Set<byte[]> zrevrangeByLex(byte[] bytes, byte[] bytes1, byte[] bytes2) {
        Set<byte[]> rtn;
        Object jedis = null;
        try {
            jedis = getResource();
            if (CLUSTER.equals(redisJedisPool.getPoolType())) {
                rtn = ((JedisCluster) jedis).zrevrangeByLex(bytes, bytes1, bytes2);
            } else {
                rtn = ((Jedis) jedis).zrevrangeByLex(bytes, bytes1, bytes2);
            }
            return rtn;
        } catch (Exception e) {
            throw e;
        } finally {
            returnResource(jedis);
        }
    }

    public Set<byte[]> zrevrangeByLex(byte[] bytes, byte[] bytes1, byte[] bytes2, int i, int i1) {
        Set<byte[]> rtn;
        Object jedis = null;
        try {
            jedis = getResource();
            if (CLUSTER.equals(redisJedisPool.getPoolType())) {
                rtn = ((JedisCluster) jedis).zrevrangeByLex(bytes, bytes1, bytes2, i, i1);
            } else {
                rtn = ((Jedis) jedis).zrevrangeByLex(bytes, bytes1, bytes2, i, i1);
            }
            return rtn;
        } catch (Exception e) {
            throw e;
        } finally {
            returnResource(jedis);
        }
    }

    public Long zremrangeByLex(byte[] key, byte[] min, byte[] max) {
        Long rtn;
        Object jedis = null;
        try {
            jedis = getResource();
            if (CLUSTER.equals(redisJedisPool.getPoolType())) {
                rtn = ((JedisCluster) jedis).zremrangeByLex(key, min, max);
            } else {
                rtn = ((Jedis) jedis).zremrangeByLex(key, min, max);
            }
            return rtn;
        } catch (Exception e) {
            throw e;
        } finally {
            returnResource(jedis);
        }
    }

    public Long lpushx(byte[] key, byte[]... arg) {
        Long rtn;
        Object jedis = null;
        try {
            jedis = getResource();
            if (CLUSTER.equals(redisJedisPool.getPoolType())) {
                rtn = ((JedisCluster) jedis).lpushx(key, arg);
            } else {
                rtn = ((Jedis) jedis).lpushx(key, arg);
            }
            return rtn;
        } catch (Exception e) {
            throw e;
        } finally {
            returnResource(jedis);
        }
    }

    public Long rpushx(byte[] key, byte[]... arg) {
        Long rtn;
        Object jedis = null;
        try {
            jedis = getResource();
            if (CLUSTER.equals(redisJedisPool.getPoolType())) {
                rtn = ((JedisCluster) jedis).rpushx(key, arg);
            } else {
                rtn = ((Jedis) jedis).rpushx(key, arg);
            }
            return rtn;
        } catch (Exception e) {
            throw e;
        } finally {
            returnResource(jedis);
        }
    }

    public List<byte[]> blpop(byte[] arg) {

        return null;
    }

    public List<byte[]> brpop(byte[] arg) {

        return null;
    }

    public Long del(byte[] key) {
        Long rtn;
        Object jedis = null;
        try {
            jedis = getResource();
            if (CLUSTER.equals(redisJedisPool.getPoolType())) {
                rtn = ((JedisCluster) jedis).del(key);
            } else {
                rtn = ((Jedis) jedis).del(key);
            }
            return rtn;
        } catch (Exception e) {
            throw e;
        } finally {
            returnResource(jedis);
        }
    }

    public byte[] echo(byte[] arg) {
        byte[] rtn;
        Object jedis = null;
        try {
            jedis = getResource();
            if (CLUSTER.equals(redisJedisPool.getPoolType())) {
                rtn = ((JedisCluster) jedis).echo(arg);
            } else {
                rtn = ((Jedis) jedis).echo(arg);
            }
            return rtn;
        } catch (Exception e) {
            throw e;
        } finally {
            returnResource(jedis);
        }
    }

    public Long move(byte[] key, int dbIndex) {
        Long rtn;
        Object jedis = null;
        try {
            jedis = getResource();
            if (CLUSTER.equals(redisJedisPool.getPoolType())) {
                rtn = ((JedisCluster) jedis).move(new String(key), dbIndex);
            } else {
                rtn = ((Jedis) jedis).move(key, dbIndex);
            }
            return rtn;
        } catch (Exception e) {
            throw e;
        } finally {
            returnResource(jedis);
        }
    }

    public Long bitcount(byte[] key) {
        Long rtn;
        Object jedis = null;
        try {
            jedis = getResource();
            if (CLUSTER.equals(redisJedisPool.getPoolType())) {
                rtn = ((JedisCluster) jedis).bitcount(key);
            } else {
                rtn = ((Jedis) jedis).bitcount(key);
            }
            return rtn;
        } catch (Exception e) {
            throw e;
        } finally {
            returnResource(jedis);
        }
    }

    public Long bitcount(byte[] key, long start, long end) {
        Long rtn;
        Object jedis = null;
        try {
            jedis = getResource();
            if (CLUSTER.equals(redisJedisPool.getPoolType())) {
                rtn = ((JedisCluster) jedis).bitcount(key, start, end);
            } else {
                rtn = ((Jedis) jedis).bitcount(key, start, end);
            }
            return rtn;
        } catch (Exception e) {
            throw e;
        } finally {
            returnResource(jedis);
        }
    }

    public Long pfadd(byte[] key, byte[]... elements) {
        Long rtn;
        Object jedis = null;
        try {
            jedis = getResource();
            if (CLUSTER.equals(redisJedisPool.getPoolType())) {
                rtn = ((JedisCluster) jedis).pfadd(key, elements);
            } else {
                rtn = ((Jedis) jedis).pfadd(key, elements);
            }
            return rtn;
        } catch (Exception e) {
            throw e;
        } finally {
            returnResource(jedis);
        }
    }

    public long pfcount(byte[] key) {
        Long rtn;
        Object jedis = null;
        try {
            jedis = getResource();
            if (CLUSTER.equals(redisJedisPool.getPoolType())) {
                rtn = ((JedisCluster) jedis).pfcount(key);
            } else {
                rtn = ((Jedis) jedis).pfcount(key);
            }
            return rtn;
        } catch (Exception e) {
            throw e;
        } finally {
            returnResource(jedis);
        }
    }

    public Long geoadd(byte[] bytes, double v, double v1, byte[] bytes1) {
        Long rtn;
        Object jedis = null;
        try {
            jedis = getResource();
            if (CLUSTER.equals(redisJedisPool.getPoolType())) {
                rtn = ((JedisCluster) jedis).geoadd(bytes, v, v1, bytes1);
            } else {
                rtn = ((Jedis) jedis).geoadd(bytes, v, v1, bytes1);
            }
            return rtn;
        } catch (Exception e) {
            throw e;
        } finally {
            returnResource(jedis);
        }
    }

    public Long geoadd(byte[] bytes, Map<byte[], GeoCoordinate> map) {
        Long rtn;
        Object jedis = null;
        try {
            jedis = getResource();
            if (CLUSTER.equals(redisJedisPool.getPoolType())) {
                rtn = ((JedisCluster) jedis).geoadd(bytes, map);
            } else {
                rtn = ((Jedis) jedis).geoadd(bytes, map);
            }
            return rtn;
        } catch (Exception e) {
            throw e;
        } finally {
            returnResource(jedis);
        }
    }

    public Double geodist(byte[] bytes, byte[] bytes1, byte[] bytes2) {
        Double rtn;
        Object jedis = null;
        try {
            jedis = getResource();
            if (CLUSTER.equals(redisJedisPool.getPoolType())) {
                rtn = ((JedisCluster) jedis).geodist(bytes, bytes1, bytes2);
            } else {
                rtn = ((Jedis) jedis).geodist(bytes, bytes1, bytes2);
            }
            return rtn;
        } catch (Exception e) {
            throw e;
        } finally {
            returnResource(jedis);
        }
    }

    public Double geodist(byte[] bytes, byte[] bytes1, byte[] bytes2, GeoUnit geoUnit) {
        Double rtn;
        Object jedis = null;
        try {
            jedis = getResource();
            if (CLUSTER.equals(redisJedisPool.getPoolType())) {
                rtn = ((JedisCluster) jedis).geodist(bytes, bytes1, bytes2, geoUnit);
            } else {
                rtn = ((Jedis) jedis).geodist(bytes, bytes1, bytes2, geoUnit);
            }
            return rtn;
        } catch (Exception e) {
            throw e;
        } finally {
            returnResource(jedis);
        }
    }

    public List<byte[]> geohash(byte[] bytes, byte[]... bytes1) {
        List<byte[]> rtn;
        Object jedis = null;
        try {
            jedis = getResource();
            if (CLUSTER.equals(redisJedisPool.getPoolType())) {
                rtn = ((JedisCluster) jedis).geohash(bytes, bytes1);
            } else {
                rtn = ((Jedis) jedis).geohash(bytes, bytes1);
            }
            return rtn;
        } catch (Exception e) {
            throw e;
        } finally {
            returnResource(jedis);
        }
    }

    public List<GeoCoordinate> geopos(byte[] bytes, byte[]... bytes1) {
        List<GeoCoordinate> rtn;
        Object jedis = null;
        try {
            jedis = getResource();
            if (CLUSTER.equals(redisJedisPool.getPoolType())) {
                rtn = ((JedisCluster) jedis).geopos(bytes, bytes1);
            } else {
                rtn = ((Jedis) jedis).geopos(bytes, bytes1);
            }
            return rtn;
        } catch (Exception e) {
            throw e;
        } finally {
            returnResource(jedis);
        }
    }

    public List<GeoRadiusResponse> georadius(byte[] bytes, double v, double v1, double v2, GeoUnit geoUnit) {
        List<GeoRadiusResponse> rtn;
        Object jedis = null;
        try {
            jedis = getResource();
            if (CLUSTER.equals(redisJedisPool.getPoolType())) {
                rtn = ((JedisCluster) jedis).georadius(bytes, v, v1, v2, geoUnit);
            } else {
                rtn = ((Jedis) jedis).georadius(bytes, v, v1, v2, geoUnit);
            }
            return rtn;
        } catch (Exception e) {
            throw e;
        } finally {
            returnResource(jedis);
        }
    }

    /*public List<GeoRadiusResponse> georadius(byte[] bytes, double v, double v1, double v2, GeoUnit geoUnit, GeoRadiusParam geoRadiusParam) {
        List<GeoRadiusResponse>  rtn;
        Object jedis = null;
        try{
            jedis = getResource();
            if(MASTER.equals(redisJedisPool.getPoolType())){
                rtn = ((MasterSlaveJedis)jedis).georadius(bytes, v, v1, v2, geoUnit,geoRadiusParam);
            }else if(CLUSTER.equals(redisJedisPool.getPoolType())){
                rtn = ((JedisCluster)jedis).georadius(bytes, v, v1, v2, geoUnit,geoRadiusParam);
            }else{
                rtn = ((Jedis) jedis).georadius(bytes, v, v1, v2, geoUnit,geoRadiusParam);
            }
            return rtn;
        }catch(Exception e){
            throw e;
        }finally {
            returnResource(jedis);
        }
    }*/
    public List<GeoRadiusResponse> georadiusByMember(byte[] bytes, byte[] bytes1, double v, GeoUnit geoUnit) {
        List<GeoRadiusResponse> rtn;
        Object jedis = null;
        try {
            jedis = getResource();
            if (CLUSTER.equals(redisJedisPool.getPoolType())) {
                rtn = ((JedisCluster) jedis).georadiusByMember(bytes, bytes1, v, geoUnit);
            } else {
                rtn = ((Jedis) jedis).georadiusByMember(bytes, bytes1, v, geoUnit);
            }
            return rtn;
        } catch (Exception e) {
            throw e;
        } finally {
            returnResource(jedis);
        }
    }

    /*public List<GeoRadiusResponse> georadiusByMember(byte[] bytes, byte[] bytes1, double v, GeoUnit geoUnit, GeoRadiusParam geoRadiusParam) {
        List<GeoRadiusResponse>  rtn;
        Object jedis = null;
        try{
            jedis = getResource();
            if(MASTER.equals(redisJedisPool.getPoolType())){
                rtn = ((MasterSlaveJedis)jedis).georadiusByMember(bytes,bytes1,v,geoUnit,geoRadiusParam);
            }else if(CLUSTER.equals(redisJedisPool.getPoolType())){
                rtn = ((JedisCluster)jedis).georadiusByMember(bytes,bytes1,v,geoUnit,geoRadiusParam);
            }else{
                rtn = ((Jedis) jedis).georadiusByMember(bytes,bytes1,v,geoUnit,geoRadiusParam);
            }
            return rtn;
        }catch(Exception e){
            throw e;
        }finally {
            returnResource(jedis);
        }
    }*/
    public String ping() {
        String rtn;
        Object jedis = null;
        try {
            jedis = getResource();
            if (CLUSTER.equals(redisJedisPool.getPoolType())) {
                rtn = ((JedisCluster) jedis).ping();
            } else {
                rtn = ((Jedis) jedis).ping();
            }
            return rtn;
        } catch (Exception e) {
            throw e;
        } finally {
            returnResource(jedis);
        }
    }

    public String quit() {
        String rtn;
        Object jedis = null;
        try {
            jedis = getResource();
            if (CLUSTER.equals(redisJedisPool.getPoolType())) {
                //                rtn = ((JedisCluster)jedis).quit();
                throw new IllegalStateException("not support");
            } else {
                rtn = ((Jedis) jedis).quit();
            }
            return rtn;
        } catch (Exception e) {
            throw e;
        } finally {
            returnResource(jedis);
        }
    }

    public Long dbSize() {
        Long rtn;
        Object jedis = null;
        try {
            jedis = getResource();
            if (CLUSTER.equals(redisJedisPool.getPoolType())) {
                //                rtn = ((JedisCluster)jedis).dbSize();
                throw new IllegalStateException("not support");
            } else {
                rtn = ((Jedis) jedis).dbSize();
            }
            return rtn;
        } catch (Exception e) {
            throw e;
        } finally {
            returnResource(jedis);
        }
    }

    public String select(int index) {
        String rtn;
        Object jedis = null;
        try {
            jedis = getResource();
            if (CLUSTER.equals(redisJedisPool.getPoolType())) {
                //                rtn = ((JedisCluster)jedis).select(index);
                throw new IllegalStateException("not support");
            } else {
                rtn = ((Jedis) jedis).select(index);
            }
            return rtn;
        } catch (Exception e) {
            throw e;
        } finally {
            returnResource(jedis);
        }
    }

    public String auth(String password) {
        String rtn;
        Object jedis = null;
        try {
            jedis = getResource();
            if (CLUSTER.equals(redisJedisPool.getPoolType())) {
                //                rtn = ((JedisCluster)jedis).auth(password);
                throw new IllegalStateException("not support");
            } else {
                rtn = ((Jedis) jedis).auth(password);
            }
            return rtn;
        } catch (Exception e) {
            throw e;
        } finally {
            returnResource(jedis);
        }
    }

    public String save() {
        String rtn;
        Object jedis = null;
        try {
            jedis = getResource();
            if (CLUSTER.equals(redisJedisPool.getPoolType())) {
                //                rtn = ((JedisCluster)jedis).save();
                throw new IllegalStateException("not support");
            } else {
                rtn = ((Jedis) jedis).save();
            }
            return rtn;
        } catch (Exception e) {
            throw e;
        } finally {
            returnResource(jedis);
        }
    }

    public String bgsave() {
        String rtn;
        Object jedis = null;
        try {
            jedis = getResource();
            if (CLUSTER.equals(redisJedisPool.getPoolType())) {
                //                rtn = ((JedisCluster)jedis).bgsave();
                throw new IllegalStateException("not support");
            } else {
                rtn = ((Jedis) jedis).bgsave();
            }
            return rtn;
        } catch (Exception e) {
            throw e;
        } finally {
            returnResource(jedis);
        }
    }

    public String bgrewriteaof() {
        String rtn;
        Object jedis = null;
        try {
            jedis = getResource();
            if (CLUSTER.equals(redisJedisPool.getPoolType())) {
                //                rtn = ((JedisCluster)jedis).bgrewriteaof();
                throw new IllegalStateException("not support");
            } else {
                rtn = ((Jedis) jedis).bgrewriteaof();
            }
            return rtn;
        } catch (Exception e) {
            throw e;
        } finally {
            returnResource(jedis);
        }
    }

    public Long lastsave() {
        Long rtn;
        Object jedis = null;
        try {
            jedis = getResource();
            if (CLUSTER.equals(redisJedisPool.getPoolType())) {
                //                rtn = ((JedisCluster)jedis).lastsave();
                throw new IllegalStateException("not support");
            } else {
                rtn = ((Jedis) jedis).lastsave();
            }
            return rtn;
        } catch (Exception e) {
            throw e;
        } finally {
            returnResource(jedis);
        }
    }

    public String shutdown() {

        return null;
    }

    public String info() {
        String rtn;
        Object jedis = null;
        try {
            jedis = getResource();
            if (CLUSTER.equals(redisJedisPool.getPoolType())) {
                //                rtn = ((JedisCluster)jedis).info();
                throw new IllegalStateException("not support");
            } else {
                rtn = ((Jedis) jedis).info();
            }
            return rtn;
        } catch (Exception e) {
            throw e;
        } finally {
            returnResource(jedis);
        }
    }

    public String info(String section) {
        String rtn;
        Object jedis = null;
        try {
            jedis = getResource();
            if (CLUSTER.equals(redisJedisPool.getPoolType())) {
                //                rtn = ((JedisCluster)jedis).info(section);
                throw new IllegalStateException("not support");
            } else {
                rtn = ((Jedis) jedis).info(section);
            }
            return rtn;
        } catch (Exception e) {
            throw e;
        } finally {
            returnResource(jedis);
        }
    }

    public String slaveof(String host, int port) {
        String rtn;
        Object jedis = null;
        try {
            jedis = getResource();
            if (CLUSTER.equals(redisJedisPool.getPoolType())) {
                //                rtn = ((JedisCluster)jedis).slaveof(host, port);
                throw new IllegalStateException("not support");
            } else {
                rtn = ((Jedis) jedis).slaveof(host, port);
            }
            return rtn;
        } catch (Exception e) {
            throw e;
        } finally {
            returnResource(jedis);
        }
    }

    public String slaveofNoOne() {
        String rtn;
        Object jedis = null;
        try {
            jedis = getResource();
            if (CLUSTER.equals(redisJedisPool.getPoolType())) {
                //                rtn = ((JedisCluster)jedis).slaveofNoOne();
                throw new IllegalStateException("not support");
            } else {
                rtn = ((Jedis) jedis).slaveofNoOne();
            }
            return rtn;
        } catch (Exception e) {
            throw e;
        } finally {
            returnResource(jedis);
        }
    }

    public Long getDB() {
        Long rtn;
        Object jedis = null;
        try {
            jedis = getResource();
            if (CLUSTER.equals(redisJedisPool.getPoolType())) {
                //                rtn = ((JedisCluster)jedis).getDB();
                throw new IllegalStateException("not support");
            } else {
                rtn = ((Jedis) jedis).getDB();
            }
            return rtn;
        } catch (Exception e) {
            throw e;
        } finally {
            returnResource(jedis);
        }
    }

    public String debug(DebugParams params) {
        String rtn;
        Object jedis = null;
        try {
            jedis = getResource();
            if (CLUSTER.equals(redisJedisPool.getPoolType())) {
                //                rtn = ((JedisCluster)jedis).debug(params);
                throw new IllegalStateException("not support");
            } else {
                rtn = ((Jedis) jedis).debug(params);
            }
            return rtn;
        } catch (Exception e) {
            throw e;
        } finally {
            returnResource(jedis);
        }
    }

    public String configResetStat() {
        String rtn;
        Object jedis = null;
        try {
            jedis = getResource();
            if (CLUSTER.equals(redisJedisPool.getPoolType())) {
                //                rtn = ((JedisCluster)jedis).configResetStat();
                throw new IllegalStateException("not support");
            } else {
                rtn = ((Jedis) jedis).configResetStat();
            }
            return rtn;
        } catch (Exception e) {
            throw e;
        } finally {
            returnResource(jedis);
        }
    }

    public Long waitReplicas(int replicas, long timeout) {
        Long rtn;
        Object jedis = null;
        try {
            jedis = getResource();
            if (CLUSTER.equals(redisJedisPool.getPoolType())) {
                //                rtn = ((JedisCluster)jedis).waitReplicas(replicas, timeout);
                throw new IllegalStateException("not support");
            } else {
                rtn = ((Jedis) jedis).waitReplicas(replicas, timeout);
            }
            return rtn;
        } catch (Exception e) {
            throw e;
        } finally {
            returnResource(jedis);
        }
    }

    public enum ExpireType {
        /**
         * EX seconds – 设置键key的过期时间，单位时秒 PX milliseconds – 设置键key的过期时间，单位时毫秒
         */
        Milliseconds("PX"), Seconds("EX");
        public String value;

        private ExpireType(String value) {
            this.value = value;
        }

        public String getValue() {
            return value;
        }

        public void setValue(String value) {
            this.value = value;
        }
    }

    public enum SetPremise {
        /**
         * NX – 只有键key不存在的时候才会设置key的值 XX – 只有键key存在的时候才会设置key的值
         */
        NX("NX"), XX("XX");
        public String value;

        private SetPremise(String value) {
            this.value = value;
        }

        public String getValue() {
            return value;
        }

        public void setValue(String value) {
            this.value = value;
        }
    }


    public Long linsert(String key, BinaryClient.LIST_POSITION where, String pivot, String value) {
        Long rtn;
        Object jedis = null;
        try {
            jedis = getResource();
            if (CLUSTER.equalsIgnoreCase(redisJedisPool.getPoolType())) {
                rtn = ((JedisCluster) jedis).linsert(key, where, pivot, value);
            } else {

                rtn = ((Jedis) jedis).linsert(key, where, pivot, value);
            }
            return rtn;
        } catch (Exception e) {
            throw e;
        } finally {
            returnResource(jedis);
        }
    }

    public Long zadd(String key, Map<String, Double> scoreMembers, ZAddParams zAddParams) {
        Long rtn;
        Object jedis = null;
        try {
            jedis = getResource();
            if (CLUSTER.equals(redisJedisPool.getPoolType())) {
                rtn = ((JedisCluster) jedis).zadd(key, scoreMembers, zAddParams);
            } else {
                rtn = ((Jedis) jedis).zadd(key, scoreMembers, zAddParams);
            }
            return rtn;
        } catch (Exception e) {
            throw e;
        } finally {
            returnResource(jedis);
        }
    }
}
