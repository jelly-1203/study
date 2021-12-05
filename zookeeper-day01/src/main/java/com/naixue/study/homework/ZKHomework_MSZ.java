package com.naixue.study.homework;

import org.apache.zookeeper.ZooKeeper;

import java.util.Map;

public interface ZKHomework_MSZ {
    /**
     * 级联查看某节点下所有节点及节点值 */
    public Map<String, String> getChildNodeAndValue(String path, ZooKeeper zk) throws Exception;
    /**
     * 删除一个节点，不管有有没有任何子节点 */
    public boolean rmr(String path, ZooKeeper zk) throws Exception;
    /**
     * 级联创建任意节点 * /a/b/c/d/e
     */
    public boolean createZNode(String znodePath, String data, ZooKeeper zk) throws Exception;
    /**
     * 清空子节点 */
    public boolean clearChildNode(String znodePath, ZooKeeper zk) throws Exception;
}

