package com.cgy.bean;

/**
 * @author GyuanYuan Cai
 * 2020/10/27
 * Description:
 */

// 定义样例类：水位传感器：用于接收空高数据
// id:传感器编号
// ts:时间戳
// vc:空高
public class WaterSensor {
    private String id;
    private Long ts;
    private Integer vc;

    public WaterSensor() {
    }

    public WaterSensor(String id, Long ts, Integer vc) {
        this.id = id;
        this.ts = ts;
        this.vc = vc;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public Long getTs() {
        return ts;
    }

    public void setTs(Long ts) {
        this.ts = ts;
    }

    public Integer getVc() {
        return vc;
    }

    public void setVc(Integer vc) {
        this.vc = vc;
    }

    @Override
    public String toString() {
        return "WaterSensor{" +
                "id='" + id + '\'' +
                ", ts=" + ts +
                ", vc=" + vc +
                '}';
    }
}

/**
 * Description:
 */

