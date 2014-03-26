package com.embracesource.config;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 动态配置文件辅助类的工厂类，在创建动态配置文件辅助类时，会订阅zk数据改变的事件
 * @author june
 *
 */
public class DynamicPropertiesHelperFactory {
	private ConfigChangeSubscriber configChangeSubscriber;
	private ConcurrentHashMap<String, DynamicPropertiesHelper> helpers = new ConcurrentHashMap<String, DynamicPropertiesHelper>();

	public DynamicPropertiesHelperFactory(
			ConfigChangeSubscriber configChangeSubscriber) {
		this.configChangeSubscriber = configChangeSubscriber;
	}

	public DynamicPropertiesHelper getHelper(String key) {
		DynamicPropertiesHelper helper = (DynamicPropertiesHelper) this.helpers
				.get(key);
		if (helper != null) {
			return helper;
		}

		return createHelper(key);
	}

	/**
	 * 
	 * @param key zk中的一个节点
	 * @return
	 */
	private DynamicPropertiesHelper createHelper(String key) {
		List<String> keys = this.configChangeSubscriber.listKeys();
		if ((keys == null) || (keys.size() == 0)) {
			return null;
		}

		if (!keys.contains(key)) {
			return null;
		}

		String initValue = this.configChangeSubscriber.getInitValue(key);
		final DynamicPropertiesHelper helper = new DynamicPropertiesHelper(initValue);
		DynamicPropertiesHelper old = (DynamicPropertiesHelper) this.helpers
				.putIfAbsent(key, helper);
		if (old != null) {
			return old;
		}

		/**
		 * 订阅zk数据改变
		 */
		this.configChangeSubscriber.subscribe(key, new ConfigChangeListener() {
			public void configChanged(String key, String value) {
				helper.refresh(value);
			}
		});
		return helper;
	}
}