package com.cgy.bean;

/**
 * @author GyuanYuan Cai
 * 2020/11/3
 * Description:
 */

public class ItemCountWithWindowEnd implements Comparable<ItemCountWithWindowEnd>{

    private Long itemID;
    private Long itemCount;
    private Long windowEnd;


    @Override
    public int compareTo(ItemCountWithWindowEnd o) {
        return o.getItemCount().intValue()-this.getItemCount().intValue();
    }

    public ItemCountWithWindowEnd() {
    }

    public ItemCountWithWindowEnd(Long itemID, Long itemCount, Long windowEnd) {
        this.itemID = itemID;
        this.itemCount = itemCount;
        this.windowEnd = windowEnd;
    }

    public Long getItemID() {
        return itemID;
    }

    public void setItemID(Long itemID) {
        this.itemID = itemID;
    }

    public Long getItemCount() {
        return itemCount;
    }

    public void setItemCount(Long itemCount) {
        this.itemCount = itemCount;
    }

    public Long getWindowEnd() {
        return windowEnd;
    }

    public void setWindowEnd(Long windowEnd) {
        this.windowEnd = windowEnd;
    }

    @Override
    public String toString() {
        return "ItemCountWithWindowEnd{" +
                "itemID=" + itemID +
                ", itemCount=" + itemCount +
                ", windowEnd=" + windowEnd +
                '}';
    }
}