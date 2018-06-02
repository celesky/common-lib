/**
 * 
 */
package com.youhaoxi.base.jedis.provider.sharded;

import com.youhaoxi.base.jedis.JedisProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.*;
import redis.clients.jedis.exceptions.JedisException;

import java.util.ArrayList;
import java.util.List;

/**
 * 标准（单服务器）redis服务提供者
 */
public class JedisShardProvider implements JedisProvider<ShardedJedis,BinaryShardedJedis> {
	
	protected static final Logger logger = LoggerFactory.getLogger(JedisShardProvider.class);

	
	public static final String MODE = "standard";

	private ThreadLocal<ShardedJedis> context = new ThreadLocal<>();
	
	private ShardedJedisPool jedisPool;
	
	private String groupName;
	

	public JedisShardProvider(String groupName, JedisPoolConfig jedisPoolConfig, String[] servers, int timeout) {
		super();
		this.groupName = groupName;
		List<JedisShardInfo> shards = buildShardInfos(servers,timeout);
		jedisPool = new ShardedJedisPool(jedisPoolConfig, shards);
	}
	
	private List<JedisShardInfo> buildShardInfos(String[] servers, int timeout){
		List<JedisShardInfo> infos = new ArrayList<>();
		for (String server : servers) {
			String[] addrs = server.split(":");
			JedisShardInfo info = new JedisShardInfo(addrs[0], Integer.parseInt(addrs[1].trim()), timeout);
			infos.add(info);
		}
		
		return infos;
	}

	public ShardedJedis get() throws JedisException {
		ShardedJedis jedis = context.get();
        if(jedis != null)return jedis;
        try {
            jedis = jedisPool.getResource();
        } catch (JedisException e) {
            if(jedis!=null){
            	jedis.close();
            }
            throw e;
        }
        context.set(jedis);
        if(logger.isTraceEnabled()){
        	logger.trace(">>get a jedis conn[{}]",jedis.toString());
        }
        return jedis;
    }
 
	@Override
	public BinaryShardedJedis getBinary() {
		return get();
	}
	
	public void release() {
		ShardedJedis jedis = context.get();
        if (jedis != null) {
        	context.remove();
        	jedis.close();
        	if(logger.isTraceEnabled()){
            	logger.trace("<<release a jedis conn[{}]",jedis.toString());
            }
        }
    }

	
	@Override
	public void destroy() throws Exception{
		jedisPool.destroy();
	}


	@Override
	public String mode() {
		return MODE;
	}

	@Override
	public String groupName() {
		return groupName;
	}

}
