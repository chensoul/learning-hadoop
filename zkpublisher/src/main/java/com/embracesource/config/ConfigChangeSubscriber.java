package com.embracesource.config;

import java.util.List;

/**
 * 配置改变的订阅者，在每一個zk文件上订阅一個监听器
 * 
 * @author june
 *
 */
public abstract interface ConfigChangeSubscriber {
	public abstract String getInitValue(String paramString);

	public abstract void subscribe(String paramString,
			ConfigChangeListener paramConfigChangeListener);

	public abstract List<String> listKeys();
}