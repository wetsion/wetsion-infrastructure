package site.wetsion.framework.infrastucture.cache.callback;

import redis.clients.jedis.Pipeline;

/**
 * @author <a href="mailto:weixin@cai-inc.com">霜华</a>
 * @date 2021/6/30 5:44 PM
 **/
public interface PiplineCallback extends Callback {

    void callback (Pipeline pipeline);
}
