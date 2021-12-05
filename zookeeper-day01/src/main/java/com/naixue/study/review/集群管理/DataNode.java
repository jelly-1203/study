package com.naixue.study.review.集群管理;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;

import java.io.IOException;

/*
* 只是节点上下线，并在server节点下创建子节点
* */
public class DataNode {
    private static ZooKeeper zk;
    private static final Integer index = 927;
    public static void main(String[] args) throws Exception {
        System.out.println("执行DataNode");
        zk = new ZooKeeper(Constant.ConnectStr, Constant.TimeOut, null);
        String result = zk.create(Constant.ChildNode + index, String.valueOf(index).getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
        System.out.println("上线一个新的DataNode， " + result);
        Thread.sleep(Long.MAX_VALUE);
        /*
        * 通过关闭执行程序模拟下线
        * */
        System.out.println(String.format("%s下线了", index));
        zk.close();
    }
}
