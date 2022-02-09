package site.wetsion.framework.infrastucture.cache.callback;

import redis.clients.jedis.Transaction;

/**
 * @author 霜华
 * @date 2021/6/30 5:45 PM
 **/
public interface TransactionCallback extends Callback {

    void callback (Transaction transaction);
}
