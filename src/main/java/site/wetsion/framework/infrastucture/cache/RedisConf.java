package site.wetsion.framework.infrastucture.cache;

import lombok.Data;
import lombok.ToString;

import java.util.List;

/**
 * @author <a href="mailto:weixin@cai-inc.com">霜华</a>
 * @date 2021/6/30 6:16 PM
 **/
@Data
@ToString
public class RedisConf {

    private String type = "pool";
    /**
     * 绑定主机地址
     */
    private List<String> host;

    /**
     * 客户端闲置多久之后关闭
     */
    private int timeout;

    /**
     * 控制一个pool可以分配多少个jedis实例
     */
    private int maxActive;

    /**
     * 控制一个pool做多有多少个idle实例
     *
     */
    private int maxIdle;

    /**
     * 控制一个pool最少有多少个空闲实例
     *
     */
    private int minIdle = 1;

    /**
     * jedis DB索引
     *
     */
    private int dbIndex = 0;

    /**
     * redis sentinel 监控主机别名
     */
    private String masterName;

    /**
     * password 密码登录
     */
    private String password;

    private Boolean testOnBorrow = true;
    /**
     * 获取从连接池获取链接等待时间,单位毫秒
     */
    private long maxWaitMillis = 10000;


}
