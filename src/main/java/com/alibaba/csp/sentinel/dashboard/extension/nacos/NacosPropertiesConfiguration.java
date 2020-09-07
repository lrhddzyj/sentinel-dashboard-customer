package com.alibaba.csp.sentinel.dashboard.extension.nacos;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * sentinel -> nacos 属性配置
 * @author lrh
 */
@ConfigurationProperties(prefix = "sentinel.nacos")
public class NacosPropertiesConfiguration {

    private String serverAddr = "localhost:8848";
//    private String dataId;
//    private String groupId = "DEFAULT_GROUP";
    private String namespace = "public";

    public String getServerAddr() {
        return serverAddr;
    }

    public void setServerAddr(String serverAddr) {
        this.serverAddr = serverAddr;
    }

    public String getNamespace() {
        return namespace;
    }

    public void setNamespace(String namespace) {
        this.namespace = namespace;
    }
}