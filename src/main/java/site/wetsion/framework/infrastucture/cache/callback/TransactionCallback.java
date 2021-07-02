package site.wetsion.framework.infrastucture.cache.callback;

import redis.clients.jedis.Transaction;

/**
 * @author <a href="mailto:weixin@cai-inc.com">霜华</a>
 * @date 2021/6/30 5:45 PM
 **/
public interface TransactionCallback extends Callback {

    void callback (Transaction transaction);
}
