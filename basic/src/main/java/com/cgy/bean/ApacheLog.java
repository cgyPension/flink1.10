package com.cgy.bean;

import org.apache.hadoop.yarn.webapp.hamlet2.Hamlet;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * @author GyuanYuan Cai
 * 2020/11/3
 * Description:
 */

public class ApacheLog {
    private String ip;
    private String userId;
    private Long eventTime;
    private String method;
    private String url;



    public ApacheLog() {
    }

    // 构造器 抛异常应该没问题
    public ApacheLog(String ip, String userId, String eventTime, String method, String url) throws ParseException {
        this.ip = ip;
        this.userId = userId;

        SimpleDateFormat sdf = new SimpleDateFormat("dd/MM/yyyy:HH:mm:SS");
        Date data = sdf.parse(eventTime);
        this.eventTime = data.getTime();

        this.method = method;
        this.url = url;
    }

    public String getIp() {
        return ip;
    }

    public void setIp(String ip) {
        this.ip = ip;
    }

    public String getUserId() {
        return userId;
    }

    public void setUserId(String userId) {
        this.userId = userId;
    }

    public Long getEventTime() {
        return eventTime;
    }

    public void setEventTime(Long eventTime) {
        this.eventTime = eventTime;
    }

    public String getMethod() {
        return method;
    }

    public void setMethod(String method) {
        this.method = method;
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    @Override
    public String toString() {
        return "ApacheLog{" +
                "ip='" + ip + '\'' +
                ", userId='" + userId + '\'' +
                ", eventTime=" + eventTime +
                ", method='" + method + '\'' +
                ", url='" + url + '\'' +
                '}';
    }
}