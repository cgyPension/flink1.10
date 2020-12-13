package com.cgy.bean;

/**
 * @author GyuanYuan Cai
 * 2020/11/4
 * Description:
 */

public class PageCountWithWindow {
    private String url;
    private Long pageCount;
    private Long windowEnd;

    public PageCountWithWindow() {
    }

    public PageCountWithWindow(String url, Long pageCount, Long windowEnd) {
        this.url = url;
        this.pageCount = pageCount;
        this.windowEnd = windowEnd;
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public Long getPageCount() {
        return pageCount;
    }

    public void setPageCount(Long pageCount) {
        this.pageCount = pageCount;
    }

    public Long getWindowEnd() {
        return windowEnd;
    }

    public void setWindowEnd(Long windowEnd) {
        this.windowEnd = windowEnd;
    }

    @Override
    public String toString() {
        return "PageCountWithWindow{" +
                "url='" + url + '\'' +
                ", pageCount=" + pageCount +
                ", windowEnd=" + windowEnd +
                '}';
    }
}