package com.naixue.study.review.集群管理;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/*
* namenode 上线之后，先回去进行选举，选出leader之后，其余的为standby，然后监听server节点下的服务变化
* */
public class NameNode {
    private static List<String> oldNodes = new ArrayList<>();
    private static ZooKeeper zk;
    public static void main(String[] args) throws Exception{
        /*
        * 首先创建表示为分布式锁的节点
        * */
        zk = new ZooKeeper(Constant.ConnectStr, Constant.TimeOut, new Watcher() {
            @Override
            public void process(WatchedEvent event) {
                System.out.println(String.format("成功创建连接？%s", event.getState() == Event.KeeperState.SyncConnected));
                String listenPath = event.getPath();
                Event.EventType type = event.getType();
                System.out.println(listenPath + "  -- " + type.toString());

                /*
                * 监听 server 节点下，子节点的变化，如果是nodeChildrenChanged，则证明有新的节点注册，或者有节点下线
                * */
                if (event.getType() == Event.EventType.NodeChildrenChanged && event.getPath().equals(Constant.ParentNode)) {
                    try {
                        List<String> currentNodes = zk.getChildren(Constant.ParentNode, false, null);
                        if (oldNodes.size() > currentNodes.size()) {
//                            证明是下线操作
                            List<String> deletes = currentNodes.stream().filter(node -> !oldNodes.contains(node)).collect(Collectors.toList());
                            System.out.println(String.format("此次下线的节点有: %s", deletes));
                        } else if (oldNodes.size() < currentNodes.size()) {
//                            新增节点操作
                            List<String> adds = oldNodes.stream().filter(node -> !currentNodes.contains(node)).collect(Collectors.toList());
                            System.out.println(String.format("此次新增的节点有：%s", adds));
                        }
                        oldNodes = currentNodes;
                        //  重复监听相同节点
                        zk.getChildren(Constant.ParentNode, true);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }

                } else if (event.getType() == Event.EventType.NodeDataChanged && event.getPath().equals(Constant.ParentNode)) {
                    /*
                    * 监听节点数据的变化的，处理逻辑
                    * */
                    System.out.println("节点内容发生改变");
                    byte[] datas = new byte[0];
                    try {
                        datas = zk.getData(Constant.ParentNode, false, null);
                        System.out.println(String.format("%s", new String(datas)));
                        zk.getData(Constant.ParentNode, true, null);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }


                }
            }
        });

        /*
        * 首先判断 parent node 节点是否存在
        * */
        Stat exists = zk.exists(Constant.ParentNode, false);
        if (exists == null) {
            zk.create(Constant.ParentNode, "shouzuo".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        }

        zk.getChildren(Constant.ParentNode, true);
        zk.getData(Constant.ParentNode, true, null);

        Thread.sleep(Integer.MAX_VALUE);
        zk.close();
    }
}
