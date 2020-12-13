package com.cgy.bean;

/**
 * @author GyuanYuan Cai
 * 2020/11/4
 * Description:
 */

public class AdClickCountWithWindowEnd {
    private Long adId;
    private String province;
    private Long adClickCount;
    private Long windowEnd;

    public AdClickCountWithWindowEnd() {
    }

    public AdClickCountWithWindowEnd(Long adId, String province, Long adClickCount, Long windowEnd) {
        this.adId = adId;
        this.province = province;
        this.adClickCount = adClickCount;
        this.windowEnd = windowEnd;
    }

    public Long getAdId() {
        return adId;
    }

    public void setAdId(Long adId) {
        this.adId = adId;
    }

    public String getProvince() {
        return province;
    }

    public void setProvince(String province) {
        this.province = province;
    }

    public Long getAdClickCount() {
        return adClickCount;
    }

    public void setAdClickCount(Long adClickCount) {
        this.adClickCount = adClickCount;
    }

    public Long getWindowEnd() {
        return windowEnd;
    }

    public void setWindowEnd(Long windowEnd) {
        this.windowEnd = windowEnd;
    }

    @Override
    public String toString() {
        return "AdClickCountWithWindowEnd{" +
                "adId=" + adId +
                ", province='" + province + '\'' +
                ", adClickCount=" + adClickCount +
                ", windowEnd=" + windowEnd +
                '}';
    }
}