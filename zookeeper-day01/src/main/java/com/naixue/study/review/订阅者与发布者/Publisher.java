package com.naixue.study.review.订阅者与发布者;

import com.naixue.study.review.constant.ZKConstants;
import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;

/*
* 发布者
* */
public class Publisher {
    private static CountDownLatch countDownLatch = new CountDownLatch(1);
    private static ZooKeeper zk;
    private static final String PUBLISH_INFO = "bigdata9876,9527,com.mazh.nx.Service9876,getName,xuzheng";
    public static void main(String[] args) throws Exception {
        zk = new ZooKeeper(ZKConstants.CONNECT_INFO, ZKConstants.TIME_OUT, new Watcher() {
            @Override
            public void process(WatchedEvent watchedEvent) {
                if (countDownLatch.getCount() > 0 && watchedEvent.getState() == Event.KeeperState.SyncConnected) {
                    countDownLatch.countDown();
                    System.out.println("创建会话链接成功");
                }
            }
        });

        countDownLatch.await();

        Stat exists = zk.exists(ZKConstants.PARENT_PATH, false);
        if (exists == null) {
            zk.create(ZKConstants.PARENT_PATH, ZKConstants.PARENT_PATH.getBytes(), ZooDefs.Ids.READ_ACL_UNSAFE, CreateMode.PERSISTENT);
        }
        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释： 发布消息
         */
        List<String> children = zk.getChildren(ZKConstants.PARENT_PATH, false);
        String[] strings = children.toArray(new String[]{});
        Arrays.sort(strings);
        String max = strings[strings.length - 1];
//        String index = max.substring(SUB_NODE_NAME.length() - 2);
        String index = "100111";
        CreateMode serviceMode = CreateMode.PERSISTENT;
        String s = zk.create(ZKConstants.SUB_NODE + index, PUBLISH_INFO.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, serviceMode);
        System.out.println(s);

        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释： 关闭会话链接
         */
        zk.close();
    }
}
