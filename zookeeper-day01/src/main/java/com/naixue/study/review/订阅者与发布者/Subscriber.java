package com.naixue.study.review.订阅者与发布者;

import com.naixue.study.review.constant.ZKConstants;
import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.awt.event.TextEvent;
import java.io.IOException;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;

/*
* 订阅者
* */
public class Subscriber {
    private static ZooKeeper zk = null;
    private static CountDownLatch countDownLatch = new CountDownLatch(1);
    private static List<String> oldNodeLists = new ArrayList<>();
    private static List<String> newNodeList = new ArrayList<>();
    public static void main(String[] args) throws Exception {
        zk = new ZooKeeper(ZKConstants.CONNECT_INFO, ZKConstants.TIME_OUT, new Watcher() {
            @Override
            public void process(WatchedEvent event) {
                if (countDownLatch.getCount() > 0 && event.getState() == Event.KeeperState.SyncConnected) {
                    try{
                        // 判断父节点是否存在
                        Stat parent_state = zk.exists(ZKConstants.PARENT_PATH, false);
                        if (parent_state == null) {
                            // 如果父节点不存在，则创建父节点
                            zk.create(ZKConstants.PARENT_PATH, ZKConstants.PARENT_PATH.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                        }
                        // 如果父节点存在，则获取旧的服务列表
                        oldNodeLists = zk.getChildren(ZKConstants.PARENT_PATH, false);
                        System.out.println("旧的子节点列表: " + oldNodeLists);
                    }catch (Exception e) {
                        e.printStackTrace();
                    }
                    countDownLatch.countDown();
                    System.out.println("创建会话链接成功");
                }
                String listen_path = event.getPath();
                Event.EventType listen_type = event.getType();
                if (listen_path.equals(ZKConstants.PARENT_PATH) && listen_type == Event.EventType.NodeChildrenChanged) {
                    System.out.println(listen_path + " 发生了 " + listen_type + " 事件");
                    //  对注册监听的事件进行处理
                    try {
                        newNodeList = zk.getChildren(listen_path, false);
                        System.out.println("new list size is " + newNodeList.size());
                        newNodeList.stream().filter(node -> !oldNodeLists.contains(node)).forEach(node -> {
                            try {
                                String newNews = new String(zk.getData(listen_type + "/" + node, false, null));
                                System.out.println("发布了新的服务：" + newNews);
                            } catch (KeeperException e) {
                                e.printStackTrace();
                            } catch (InterruptedException e) {
                                e.printStackTrace();
                            }
                        });
                        oldNodeLists = newNodeList;
                        // 重复注册监听
                        zk.getChildren(ZKConstants.PARENT_PATH, true);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            }
        });

        countDownLatch.await();

        System.out.println("main线程被唤醒并执行");

        Stat exists = zk.exists(ZKConstants.PARENT_PATH, false);
        if (exists == null) {
            zk.create(ZKConstants.PARENT_PATH, ZKConstants.PARENT_PATH.getBytes(), ZooDefs.Ids.READ_ACL_UNSAFE, CreateMode.PERSISTENT);
        }

        zk.getChildren(ZKConstants.PARENT_PATH, true);

        Thread.sleep(Integer.MAX_VALUE);

        zk.close();
    }
}
