package com.youhaoxi.base.spring;

import java.util.Map;

/**
 * 实例提供者接口。该接口封装了IoC工厂的具体实现。它抽象
 * 出IoC工厂的基本能力：提供某种指定类型（接口/类）的一个
 * 实例。
 *
 */
public interface InstanceProvider {

	/**
	 * 获取指定接口或基类的实现类或子类的实例
	 * @param <T> 类型参数
	 * @param beanClass 实例的类型
	 * @return 指定类型的实例。
	 */
	<T> T getInstance(Class<T> beanClass);

	/**
	 * 根据beanName指定的名字获取指定接口或基类的实现类或子类的实例
	 * @param <T> 类型参数
	 * @param beanName 实现类在容器中配置的名字
	 * @param beanClass 实例的类型
	 * @return 指定类型的实例。
	 */
	<T> T getInstance(Class<T> beanClass, String beanName);
	
	/**
	 * 根据beanName指定的名字获取实例
	 * @param <T> 类型参数
	 * @param beanName 实现类在容器中配置的名字
	 * @return 指定类型的实例。
	 */
	<T> T getInstance(String beanName);
	
	/**
	 * 获取某个接口实现个数
	 * @param beanClass
	 * @return
	 */
	<T> int getInterfaceCount(Class<T> beanClass);
	
	<T> Map<String, T> getInterfaces(Class<T> beanClass);
}
