package com.alibaba.otter.shared.common.model.config.data.mq;

import com.alibaba.otter.shared.common.model.config.data.DataMediaSource;
import com.google.common.base.Objects;

/**
 * Creator: laizhicheng
 * DateTime: 2020/9/24 15:51
 * Description: No Description
 */
public class RabbitMqMediaSource extends DataMediaSource {

    private static final long serialVersionUID = -1699317916850638143L;

    private String url;
    private String virtualHost;
    private String username;
    private String password;

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public String getVirtualHost() {
        return virtualHost;
    }

    public void setVirtualHost(String virtualHost) {
        this.virtualHost = virtualHost;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (!super.equals(o)) return false;
        RabbitMqMediaSource that = (RabbitMqMediaSource) o;
        return Objects.equal(url, that.url) &&
                Objects.equal(virtualHost, that.virtualHost) &&
                Objects.equal(username, that.username) &&
                Objects.equal(password, that.password);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(super.hashCode(), url, virtualHost, username, password);
    }
}
