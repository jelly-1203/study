package com.naixue.study.review.constant;

public class ZKConstants {
    public static final String CONNECT_INFO = "bigdata02:2181,bigdata03:2181,bigdata04:2181";
    public static final int TIME_OUT = 4000;
    // 测试使用的根节点
    public static final String ROOT_PATH = "/nxRoot";
    // 发布订阅测试使用的父节点
    public static String PARENT_PATH = "/publish_parent";

    public static final String SUB_NODE_NAME = "publish_info";

    public static final String SUB_NODE = PARENT_PATH + "/" + SUB_NODE_NAME;

}
