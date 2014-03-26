package com.embracesource.config;

import java.io.IOException;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;

public class ZkUsageTest {
	public static void main(String[] args) throws IOException, KeeperException, InterruptedException {

		// 创建一个与服务器的连接
		ZooKeeper zk = new ZooKeeper("localhost:" + 2181, 3000, new Watcher() {
			// 监控所有被触发的事件
			public void process(WatchedEvent event) {
				System.out.println("已经触发了" + event.getType() + "事件！");
			}
		});
		// 创建一个目录节点
		zk.create("/zkSample", "testRootData".getBytes(),
				Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
		// 创建一个子目录节点
		zk.create("/zkSample/conf",
				"testChildDataOne".getBytes(), Ids.OPEN_ACL_UNSAFE,
				CreateMode.PERSISTENT);
		System.out
				.println(new String(zk.getData("/zkSample", false, null)));
		// 取出子目录节点列表
		System.out.println(zk.getChildren("/zkSample", true));
		// 修改子目录节点数据
		zk.setData("/zkSample/conf",
				"modifyChildDataOne".getBytes(), -1);
		System.out.println("目录节点状态：[" + zk.exists("/zkSample", true) + "]");
		// 创建另外一个子目录节点
		zk.create("/zkSample/testChildPathTwo",
				"testChildDataTwo".getBytes(), Ids.OPEN_ACL_UNSAFE,
				CreateMode.PERSISTENT);
		System.out.println(new String(zk.getData(
				"/zkSample/testChildPathTwo", true, null)));
		// 删除子目录节点
		zk.delete("/zkSample/testChildPathTwo", -1);
		zk.delete("/zkSample/conf", -1);
		// 删除父目录节点
		zk.delete("/zkSample", -1);
		// 关闭连接
		zk.close();
	}
}
