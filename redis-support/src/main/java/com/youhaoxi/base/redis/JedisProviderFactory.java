/**
 * 
 */
package com.youhaoxi.base.redis;

import com.youhaoxi.base.redis.provider.cluster.JedisClusterProvider;
import com.youhaoxi.base.redis.provider.standard.JedisStandardProvider;
import com.youhaoxi.base.spring.InstanceFactory;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.Assert;
import redis.clients.jedis.*;

import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * redis实例工厂
 *
 * 从spring容器中获取redisProvider 并放入map中管理
 * 
 * @description <br>
 */
public class JedisProviderFactory {

	protected static final Logger logger = LoggerFactory.getLogger(JedisProviderFactory.class);

	private static JedisProvider<?, ?> defaultJedisProvider;

	private static Map<String, JedisProvider> jedisProviders = new ConcurrentHashMap<>();

	public static JedisProvider<?, ?> getJedisProvider(String groupName) {
		if(defaultJedisProvider == null){			
			initFactoryFromSpring();
		}
		if(StringUtils.isNotBlank(groupName)){
			if(jedisProviders.containsKey(groupName)){				
				return jedisProviders.get(groupName);
			}else{
				logger.warn("未找到group[{}]对应的redis配置，使用默认缓存配置",groupName);
			}
		}
		return defaultJedisProvider;
	}

	private synchronized static void initFactoryFromSpring() {
		if(defaultJedisProvider == null){
			//阻塞，直到spring初始化完成
			InstanceFactory.waitUtilInitialized();
			
			Map<String, JedisProvider> interfaces = InstanceFactory.getInstanceProvider().getInterfaces(JedisProvider.class);
			Iterator<JedisProvider> iterator = interfaces.values().iterator();
			while(iterator.hasNext()){
				JedisProvider jp = iterator.next();
				jedisProviders.put(jp.groupName(), jp);
			}
			defaultJedisProvider = jedisProviders.get(JedisProviderFactoryBean.DEFAULT_GROUP_NAME);
			if(defaultJedisProvider == null && jedisProviders.size() == 1){
				defaultJedisProvider = InstanceFactory.getInstance(JedisProvider.class);
			}
			
			Assert.notNull(defaultJedisProvider,"无默认缓存配置，请指定一组缓存配置group为default");
		}
	}

	public static JedisCommands getJedisCommands(String groupName) {
		return (JedisCommands) getJedisProvider(groupName).get();
	}

	public static BinaryJedisCommands getBinaryJedisCommands(String groupName) {
		return (BinaryJedisCommands) getJedisProvider(groupName).getBinary();
	}

	public static BinaryJedisClusterCommands getBinaryJedisClusterCommands(String groupName) {
		return (BinaryJedisClusterCommands) getJedisProvider(groupName).getBinary();
	}

	public static JedisCommands getJedisClusterCommands(String groupName) {
		return (JedisCommands) getJedisProvider(groupName).get();
	}
	
	public static MultiKeyCommands getMultiKeyCommands(String groupName) {
		return (MultiKeyCommands) getJedisProvider(groupName).get();
	}
	
	public static MultiKeyBinaryCommands getMultiKeyBinaryCommands(String groupName) {
		return (MultiKeyBinaryCommands) getJedisProvider(groupName).get();
	}
	
	public static MultiKeyJedisClusterCommands getMultiKeyJedisClusterCommands(String groupName) {
		return (MultiKeyJedisClusterCommands) getJedisProvider(groupName).get();
	}
	
	public static MultiKeyBinaryJedisClusterCommands getMultiKeyBinaryJedisClusterCommands(String groupName) {
		return (MultiKeyBinaryJedisClusterCommands) getJedisProvider(groupName).get();
	}
	
	public static String currentMode(String groupName){
		return getJedisProvider(groupName).mode();
	}
	
	public static boolean isStandard(String groupName){
		return JedisStandardProvider.MODE.equals(currentMode(groupName));
	}
	
	public static boolean isCluster(String groupName){
		return JedisClusterProvider.MODE.equals(currentMode(groupName));
	}
}
