package com.missouri.realtime.bean;

import com.alibaba.fastjson.JSON;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;
import java.text.ParseException;
import java.text.SimpleDateFormat;

/**
 * @author Missouri
 * @date 2021/8/2 19:56
 */
//目标宽表
@Data
@AllArgsConstructor
@NoArgsConstructor
public class OrderWide {
    private Long detail_id;
    private Long order_id;
    private Long sku_id;
    private BigDecimal order_price;
    private Long sku_num;
    private String sku_name;
    private Long province_id;
    private String order_status;
    private Long user_id;

    private BigDecimal total_amount;
    private BigDecimal activity_reduce_amount;
    private BigDecimal coupon_reduce_amount;
    private BigDecimal original_total_amount;
    private BigDecimal feight_fee;
    private BigDecimal split_feight_fee;
    private BigDecimal split_activity_amount;
    private BigDecimal split_coupon_amount;
    private BigDecimal split_total_amount;

    private String expire_time;
    private String create_time;
    private String operate_time;
    private String create_date; // 把其他字段处理得到
    private String create_hour;

    private String province_name;//查询维表得到
    private String province_area_code;
    private String province_iso_code;
    private String province_3166_2_code;

    private Integer user_age;
    private String user_gender;

    private Long spu_id;     //作为维度数据 要关联进来
    private Long tm_id;
    private Long category3_id;
    private String spu_name;
    private String tm_name;
    private String category3_name;

    public OrderWide(OrderInfo orderInfo, OrderDetail orderDetail) {
        mergeOrderInfo(orderInfo);
        mergeOrderDetail(orderDetail);

    }

    public void mergeOrderInfo(OrderInfo orderInfo) {
        if (orderInfo != null) {
            this.order_id = orderInfo.getId();
            this.order_status = orderInfo.getOrder_status();
            this.create_time = orderInfo.getCreate_time();
            this.create_date = orderInfo.getCreate_date();
            this.create_hour = orderInfo.getCreate_hour();
            this.activity_reduce_amount = orderInfo.getActivity_reduce_amount();
            this.coupon_reduce_amount = orderInfo.getCoupon_reduce_amount();
            this.original_total_amount = orderInfo.getOriginal_total_amount();
            this.feight_fee = orderInfo.getFeight_fee();
            this.total_amount = orderInfo.getTotal_amount();
            this.province_id = orderInfo.getProvince_id();
            this.user_id = orderInfo.getUser_id();
        }
    }

    public void mergeOrderDetail(OrderDetail orderDetail) {
        if (orderDetail != null) {
            this.detail_id = orderDetail.getId();
            this.sku_id = orderDetail.getSku_id();
            this.sku_name = orderDetail.getSku_name();
            this.order_price = orderDetail.getOrder_price();
            this.sku_num = orderDetail.getSku_num();
            this.split_activity_amount = orderDetail.getSplit_activity_amount();
            this.split_coupon_amount = orderDetail.getSplit_coupon_amount();
            this.split_total_amount = orderDetail.getSplit_total_amount();
        }
    }

    public void setUser_age(String birthday){
        try {
            long bir = new SimpleDateFormat("yyyy-MM-dd").parse(birthday).getTime();
            this.user_age = Math.toIntExact((System.currentTimeMillis() - bir) / 1000 / 60 / 60 / 24 / 365);
        } catch (ParseException e) {
            e.printStackTrace();
        }
    }

    public String toJsonString(){
        return JSON.toJSONString(this);
    }

}
