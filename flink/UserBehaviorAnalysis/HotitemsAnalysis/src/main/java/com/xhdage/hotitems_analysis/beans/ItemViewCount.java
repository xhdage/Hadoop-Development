package com.xhdage.hotitems_analysis.beans;

// 商品访问量
public class ItemViewCount {
    private Long itemId;
    private Long WindowEnd;
    private Long count;

    public ItemViewCount() {
    }

    public ItemViewCount(Long itemId, Long windowEnd, Long count) {
        this.itemId = itemId;
        WindowEnd = windowEnd;
        this.count = count;
    }

    public Long getItemId() {
        return itemId;
    }

    public void setItemId(Long itemId) {
        this.itemId = itemId;
    }

    public Long getWindowEnd() {
        return WindowEnd;
    }

    public void setWindowEnd(Long windowEnd) {
        WindowEnd = windowEnd;
    }

    public Long getCount() {
        return count;
    }

    public void setCount(Long count) {
        this.count = count;
    }

    @Override
    public String toString() {
        return "ItemViewCount{" +
                "itemId=" + itemId +
                ", WindowEnd=" + WindowEnd +
                ", count=" + count +
                '}';
    }
}
