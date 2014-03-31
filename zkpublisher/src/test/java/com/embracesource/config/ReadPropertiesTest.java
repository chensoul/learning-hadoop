//package com.embracesource.config;
//
//import junit.framework.TestCase;
//
//import org.springframework.context.support.ClassPathXmlApplicationContext;
//
//public class ReadPropertiesTest extends TestCase {
//	private DynamicPropertiesHelperFactory helperFactory;
//
//	protected void setUp() throws Exception {
//		super.setUp();
//		ClassPathXmlApplicationContext ctx = new ClassPathXmlApplicationContext(
//				"test-spring-config.xml");
//		this.helperFactory = ((DynamicPropertiesHelperFactory) ctx
//				.getBean(DynamicPropertiesHelperFactory.class));
//	}
//
//	public void testReadProperties() throws InterruptedException {
//		DynamicPropertiesHelper helper = this.helperFactory
//				.getHelper("test.properties");
//
//		while (true) {
//			System.out.println(helper.getProperty("a"));
//			Thread.sleep(5000L);
//		}
//
//	}
//}
