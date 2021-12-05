package com.naixue.study.app2.集群管理;

public class Constant {

	// 请求zookeeper连接的连接信息（主机名 + 端口号）
	public static final String ConnectStr = "8.130.11.30:2181,8.130.13.228:2181,8.130.13.116:2181";
	
	// 请求连接的超时时长（单位：毫秒）
	public static final int TimeOut = 5000;
	
	// namenode监控的服务器列表的根节点
	public static final String ParentNode = "/servers";
	
	// datanode上线之后存储在zookeeper文件系统上的数据格式
	public static final String ChildNode = ParentNode + "/childNode";
}
