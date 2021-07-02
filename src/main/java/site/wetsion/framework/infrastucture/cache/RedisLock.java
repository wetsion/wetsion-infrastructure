package site.wetsion.framework.infrastucture.cache;


import com.google.common.collect.Lists;

import java.util.concurrent.TimeUnit;

import org.springframework.data.redis.core.TimeoutUtils;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import site.wetsion.framework.infrastucture.cache.util.RedisUtil;

/**
 * @author <a href="mailto:weixin@cai-inc.com">霜华</a>
 * @date 2021/6/30 1:47 PM
 **/
public class RedisLock {
    private static final String PREFIX = "gp.lock:";
    private final String key;
    private final long timeout;

    private static final String LOCK_SCRIPT =
            "if redis.call('setnx',KEYS[1],ARGV[1]) == 1 then return redis.call('expire',KEYS[1],ARGV[2]) else return 0 end";
    private static final String RELEASE_SCRIPT =
            "if redis.call('get',KEYS[1]) == ARGV[1] then return redis.call('del',KEYS[1]) else return 0 end";

    public RedisLock(String key, long timeoutSeconds) {
        this(key, timeoutSeconds, TimeUnit.SECONDS);
    }

    public RedisLock(String key, final long timeout, final TimeUnit unit) {
        this.key = PREFIX + key;
        this.timeout = TimeoutUtils.toMillis(timeout, unit);
    }

    public boolean tryLock() {

        final long value = RedisUtil.execute(new DefaultRedisScript<>(LOCK_SCRIPT, String.class),
                Lists.newArrayList(key), "true", String.valueOf(timeout));
        return 1 == value;
    }

    /**
     * 自旋锁
     */
    public boolean tryWaitLock(final long waitTimeoutSeconds) {
        return this.tryWaitLock(waitTimeoutSeconds, TimeUnit.SECONDS);
    }

    @SuppressWarnings("WeakerAccess")
    public boolean tryWaitLock(final long waitTimeout, final TimeUnit unit) {
        final long start = System.currentTimeMillis();
        final long maxWait = unit.toMillis(waitTimeout);
        int tryTime = 0;
        while ((System.currentTimeMillis() - start) < maxWait) {
            if (this.tryLock()) {
                return true;
            }
            try {
                final long sleepTime = Math.min(100 * (1 << tryTime), 5000);
                // 休眠后超时
                if ((System.currentTimeMillis() - start + sleepTime) >= maxWait) {
                    return false;
                }
                // 随机时间防止活锁
                TimeUnit.MILLISECONDS.sleep(sleepTime);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return false;
            }
            tryTime++;
        }
        return false;
    }

    public void release() {

        RedisUtil.execute(new DefaultRedisScript<>(RELEASE_SCRIPT, String.class), Lists.newArrayList(key), "true");
    }
}
