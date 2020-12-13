package com.cgy.bean;




public class AdClickCountByUserWithWindowEnd {
    private Long adId;
    private Long userId;
    private Long adClickCount;
    private Long windowEnd;

    public AdClickCountByUserWithWindowEnd() {
    }

    public AdClickCountByUserWithWindowEnd(Long adId, Long userId, Long adClickCount, Long windowEnd) {
        this.adId = adId;
        this.userId = userId;
        this.adClickCount = adClickCount;
        this.windowEnd = windowEnd;
    }

    public Long getAdId() {
        return adId;
    }

    public void setAdId(Long adId) {
        this.adId = adId;
    }

    public Long getUserId() {
        return userId;
    }

    public void setUserId(Long userId) {
        this.userId = userId;
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
        return "AdClickCountByUserWithWindowEnd{" +
                "adId=" + adId +
                ", userId=" + userId +
                ", adClickCount=" + adClickCount +
                ", windowEnd=" + windowEnd +
                '}';
    }
}
