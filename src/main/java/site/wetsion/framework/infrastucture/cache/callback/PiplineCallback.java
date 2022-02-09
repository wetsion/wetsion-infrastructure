package site.wetsion.framework.infrastucture.cache.callback;

import redis.clients.jedis.Pipeline;

/**
 * @author 霜华
 * @date 2021/6/30 5:44 PM
 **/
public interface PiplineCallback extends Callback {

    void callback (Pipeline pipeline);
}
