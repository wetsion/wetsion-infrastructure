package site.wetsion.framework.infrastucture.cache.util;

/**
 * @author <a href="mailto:weixin@cai-inc.com">霜华</a>
 * @date 2021/6/30 5:59 PM
 **/
import cn.gov.zcy.spring.boot.redis.ms.JedisClientUtil;
import cn.gov.zcy.spring.boot.redis.util.SerializeUtil;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import org.apache.commons.collections4.CollectionUtils;
import org.springframework.data.redis.connection.jedis.JedisConverters;
import org.springframework.data.redis.core.TimeoutUtils;
import org.springframework.data.redis.core.script.RedisScript;

public final class RedisUtil {
    private static JedisClientUtil jedisClient;

    public static <T> T get(String key, Class<T> clazz) {
        byte[] rawKey = rawKey(key);
        byte[] rawValue = jedisClient.get(rawKey);
        return rawValue(rawValue, clazz);
    }

    public static <T> void set(String key, T value) {
        byte[] rawKey = rawKey(key);
        byte[] rawValue = rawValue(value);
        jedisClient.set(rawKey, rawValue);
    }

    public static <T> void set(String key, T value, long expireTime, TimeUnit unit) {
        byte[] rawKey = rawKey(key);
        byte[] rawValue = rawValue(value);
        long rawTimeout = TimeoutUtils.toSeconds(expireTime, unit);
        jedisClient.setex(rawKey, (int)rawTimeout, rawValue);
    }

    public static <T> boolean setIfAbsent(String key, T value) {
        byte[] rawKey = rawKey(key);
        byte[] rawValue = rawValue(value);
        Boolean result = JedisConverters.toBoolean(jedisClient.setnx(rawKey, rawValue));
        return Boolean.TRUE.equals(result);
    }

    public static void delete(String key) {
        byte[] rawKey = rawKey(key);
        jedisClient.del(rawKey);
    }

    public static Long incr(String key) {
        byte[] rawKey = rawKey(key);
        return jedisClient.incr(rawKey);
    }

    public static void deleteFromQueue(String key, long count, Object value) {
        byte[] rawKey = rawKey(key);
        byte[] rawValue = rawValue(value);
        jedisClient.lrem(rawKey, count, rawValue);
    }

    public static void deleteFromZSet(String key, Object... values) {
        byte[] rawKey = rawKey(key);
        byte[][] rawValues = rawValues(values);
        jedisClient.zrem(rawKey, rawValues);
    }

    public static void deleteFromZSet(String key, String... values) {
        jedisClient.zrem(key, values);
    }

    public static void rangeFromZSet(String key, long start, long end) {
        byte[] rawKey = rawKey(key);
        jedisClient.zrange(rawKey, start, end);
    }

    public static boolean exists(String key) {
        byte[] rawKey = rawKey(key);
        Boolean result = jedisClient.exists(rawKey);
        return Boolean.TRUE.equals(result);
    }

    public static boolean expire(String key, long millis) {
        return expire(key, millis, TimeUnit.MILLISECONDS);
    }

    public static boolean expire(String key, long expireTime, TimeUnit unit) {
        byte[] rawKey = rawKey(key);

        try {
            long rawTimeout = TimeoutUtils.toMillis(expireTime, unit);
            Boolean result = JedisConverters.toBoolean(jedisClient.pexpire(rawKey, rawTimeout));
            return Boolean.TRUE.equals(result);
        } catch (Exception var9) {
            long seconds = TimeoutUtils.toSeconds(expireTime, unit);
            Boolean result = JedisConverters.toBoolean(jedisClient.expire(rawKey, (int)seconds));
            return Boolean.TRUE.equals(result);
        }
    }

    public static <T> void addQueue(String key, T value) {
        if (Objects.nonNull(value)) {
            byte[] rawKey = rawKey(key);
            byte[] rawValue = rawValue(value);
            jedisClient.lpush(rawKey, new byte[][]{rawValue});
        }

    }

    public static <T> void addQueue(String key, Collection<T> values) {
        if (CollectionUtils.isNotEmpty(values)) {
            byte[] rawKey = rawKey(key);
            byte[][] rawValues = rawValues(values.toArray());
            jedisClient.lpush(rawKey, rawValues);
        }

    }

    public static <T> T popQueue(String key, Class<T> clazz) {
        byte[] rawKey = rawKey(key);
        byte[] value = jedisClient.rpop(rawKey);
        return rawValue(value, clazz);
    }

    public static <T> T popQueueToBack(String sourceKey, String destinationKey, Class<T> clazz) {
        byte[] rawKey = rawKey(sourceKey);
        byte[] rawDesKey = rawKey(destinationKey);
        byte[] value = jedisClient.rpoplpush(rawKey, rawDesKey);
        return rawValue(value, clazz);
    }

    public static long sizeQueue(String key) {
        byte[] rawKey = rawKey(key);
        Long length = jedisClient.llen(rawKey);
        return (Long)Optional.ofNullable(length).orElse(0L);
    }

    public static long sizeZSet(String key) {
        byte[] rawKey = rawKey(key);
        Long length = jedisClient.zcard(rawKey);
        return (Long)Optional.ofNullable(length).orElse(0L);
    }

    public static void putHash(String key, String field, String value) {
        jedisClient.hset(key, field, value);
    }

    public static void putHashNX(String key, String field, String value) {
        jedisClient.hsetnx(key, field, value);
    }

    public static String getHash(String key, String field) {
        return jedisClient.hget(key, field);
    }

    public static Map<String, String> getHash(String key) {
        return jedisClient.hgetAll(key);
    }

    public static Long deleteHash(String key, String... field) {
        return jedisClient.hdel(key, field);
    }

    public static <T> T execute(RedisScript<String> script, List<String> keys, String... args) {
        return jedisClient.eval(script.getScriptAsString(), keys, new ArrayList(Arrays.asList(args)));
    }

    private static byte[] rawKey(String key) {
        return SerializeUtil.serialize(key);
    }

    private static byte[] rawValue(Object value) {
        return SerializeUtil.serialize(value);
    }

    private static <T> T rawValue(byte[] value, Class<T> clazz) {
        return value != null && value.length > 0 ? SerializeUtil.deserialize(value, clazz) : null;
    }

    private static byte[][] rawValues(Object... values) {
        byte[][] rawValues = new byte[values.length][];
        int i = 0;
        Object[] var3 = values;
        int var4 = values.length;

        for(int var5 = 0; var5 < var4; ++var5) {
            Object value = var3[var5];
            rawValues[i++] = rawValue(value);
        }

        return rawValues;
    }

    public static void setRedisClient(JedisClientUtil jedisClient) {
        jedisClient = jedisClient;
    }

    private RedisUtil() {
        throw new UnsupportedOperationException("This is a utility class and cannot be instantiated");
    }
}
