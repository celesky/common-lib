/**
 * 
 */
package com.youhaoxi.base.redis;

import org.springframework.beans.factory.DisposableBean;

/**
 * @description <br>
 */
public interface JedisProvider<S,B> extends DisposableBean{

	public S get();
	
	public B getBinary();
	
	public void release();
	
	public String mode();
	
	public String groupName();

}
