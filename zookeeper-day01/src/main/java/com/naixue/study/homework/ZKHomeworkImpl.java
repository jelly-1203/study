package com.naixue.study.homework;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ZKHomeworkImpl implements ZKHomework_MSZ {
    @Override
    public Map<String, String> getChildNodeAndValue(String path, ZooKeeper zk) throws Exception {
        Map<String, String> result = new HashMap<>();
        List<String> childrens = zk.getChildren(path, false);
        childrens.stream().forEach(p -> {
            if (p != null) {
                byte[] data = new byte[0];
                try {
                    data = zk.getData(p, false, null);
                } catch (Exception e) {
                    e.printStackTrace();
                }
                result.put(p, new String(data));
            }
        });
        return result;
    }

    @Override
    public boolean rmr(String path, ZooKeeper zk) throws Exception {
        return false;
    }

    @Override
    public boolean createZNode(String znodePath, String data, ZooKeeper zk) throws Exception {
        return false;
    }

    @Override
    public boolean clearChildNode(String znodePath, ZooKeeper zk) throws Exception {
        return false;
    }
}
