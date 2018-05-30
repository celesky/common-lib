package com.youhaoxi.base.spring;

import javax.servlet.ServletContextEvent;

import org.springframework.web.context.ContextLoaderListener;
import org.springframework.web.context.WebApplicationContext;
import org.springframework.web.context.support.WebApplicationContextUtils;


/** 
* 初始化一个spring provider Instance
*/
public class ExContextLoaderListener extends ContextLoaderListener {

	/* (non-Javadoc)
	 * @see org.springframework.web.context.ContextLoaderListener#contextInitialized(javax.servlet.ServletContextEvent)
	 */
	@Override
	public void contextInitialized(ServletContextEvent event) {
//		String serviceName = event.getServletContext().getInitParameter("appName");
//		System.setProperty("serviceName", serviceName == null ? "undefined" : serviceName);
//
//		String crosAllowOrigin = event.getServletContext().getInitParameter("crosAllowOrigin");
//		System.setProperty("crosAllowOrigin", crosAllowOrigin == null ? "*" : crosAllowOrigin);

		super.contextInitialized(event);
		WebApplicationContext applicationContext = WebApplicationContextUtils.getRequiredWebApplicationContext(event.getServletContext());
		SpringInstanceProvider provider = new SpringInstanceProvider(applicationContext);
		InstanceFactory.setInstanceProvider(provider);
		//先确认jvm参数是否设置了日志级别
	    String logLevel = System.getProperty("log.level");
		if(logLevel == null)System.setProperty("log.level","INFO");
	}
}
