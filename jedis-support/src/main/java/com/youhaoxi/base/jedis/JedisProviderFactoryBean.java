/**
 * 
 */
package com.youhaoxi.base.jedis;

import com.youhaoxi.base.jedis.provider.cluster.JedisClusterProvider;
import com.youhaoxi.base.jedis.provider.standard.JedisStandardProvider;
import org.apache.commons.lang3.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.support.BeanDefinitionBuilder;
import org.springframework.beans.factory.support.DefaultListableBeanFactory;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.util.StringUtils;
import redis.clients.jedis.JedisPoolConfig;

import java.util.regex.Pattern;

/**
 * redis服务提供者注册工厂
 * 把redisProvider注册到spring中
 * 需要在spring.xml中为这个bean配置初始化参数
 * @description <br>
 */
public class JedisProviderFactoryBean implements ApplicationContextAware,InitializingBean,DisposableBean {

	protected static final Logger logger = LoggerFactory.getLogger(JedisProviderFactoryBean.class);

    private static ApplicationContext applicationContext = null;

    /**
	 * 
	 */
	public static final String DEFAULT_GROUP_NAME = "default";
	
	private static final String REDIS_PROVIDER_SUFFIX = "RedisProvider";
	
	private Pattern pattern = Pattern.compile("^.+[:]\\d{1,5}\\s*$");

	private String mode;
	
	private JedisPoolConfig jedisPoolConfig;
	
	//用来区分不同组的缓存
	private String group;
	private String servers;
	private Integer timeout;
	
	private ApplicationContext context;

	public void setGroup(String group) {
		this.group = group;
	}

	public String getGroup() {
		if(group == null)group = DEFAULT_GROUP_NAME;
		return group;
	}

	public void setMode(String mode) {
		this.mode = mode;
	}

	public void setJedisPoolConfig(JedisPoolConfig jedisPoolConfig) {
		this.jedisPoolConfig = jedisPoolConfig;
	}

	public void setServers(String servers) {
		this.servers = servers;
	}

	public void setTimeout(Integer timeout) {
		this.timeout = timeout;
	}
	
	@Override
	public void setApplicationContext(ApplicationContext context) throws BeansException {
		this.context = context;
	}

	/*
	 * 实现DisposableBean接口, 在Context关闭时清理静态变量.
     */
    @Override
    public void destroy() throws Exception {
        applicationContext = null;
    }


	/**
	 * 参数设置完之后执行 注册redisProvider到spring容器中
	 * @throws Exception
	 */
	@Override
	public void afterPropertiesSet() throws Exception {
		if(jedisPoolConfig == null)throw new Exception("jedisPoolConfig Not config ??");
		if(org.apache.commons.lang3.StringUtils.isAnyBlank(mode,servers)){
			throw new Exception("type or servers is empty??");
		}
		registerRedisProvier(); 
	}

	/**
	 * 
	 */
	private void registerRedisProvier() {
		String beanName = getGroup() + REDIS_PROVIDER_SUFFIX;
		if(context.containsBean(beanName)){
			throw new RuntimeException("已包含group为［"+this.group+"］的缓存实例");
		}
		
		String[] servers = StringUtils.tokenizeToStringArray(this.servers, ConfigurableApplicationContext.CONFIG_LOCATION_DELIMITERS);
		
		//检查ip和port格式
        for (String server : servers) {
			if(!pattern.matcher(server).matches()){
				throw new RuntimeException("参数servers："+this.servers+"错误");
			}
		}
        
		Class<?> beanClass = null;
		if(JedisStandardProvider.MODE.equalsIgnoreCase(mode)){	
			beanClass = JedisStandardProvider.class;
		}else if(JedisClusterProvider.MODE.equalsIgnoreCase(mode)){
			beanClass = JedisClusterProvider.class;
		}else{
			throw new RuntimeException("参数mode："+this.mode+"不支持");
		}
		
		//注册prov
		DefaultListableBeanFactory acf = (DefaultListableBeanFactory) context.getAutowireCapableBeanFactory();  
		BeanDefinitionBuilder beanDefinitionBuilder = BeanDefinitionBuilder.genericBeanDefinition(beanClass);
		beanDefinitionBuilder.addConstructorArgValue(getGroup()).addConstructorArgValue(jedisPoolConfig).addConstructorArgValue(servers).addConstructorArgValue(timeout);
		acf.registerBeanDefinition(beanName, beanDefinitionBuilder.getRawBeanDefinition());
		//
		logger.info("register JedisProvider OK,Class:{},beanName:{}",beanClass.getSimpleName(),beanName);
	}


    /**


    /**
     * 检查ApplicationContext不为空.
     */
    private static void assertContextInjected() {
        Validate.validState(applicationContext != null,
                "applicaitonContext属性未注入, 请在applicationContext.xml中定义SpringContextHolder.");
    }
}
