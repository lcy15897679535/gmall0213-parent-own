package com.atguigu.gamll0213publisher.mapper;

import com.atguigu.gamll0213publisher.bean.HourAmount;

import java.math.BigDecimal;
import java.util.List;
import java.util.Map;


/**
 * @author lcy
 * @create 2020-07-31 15:06
 */
public interface OrderWideMapper {


    //查询总额
    public BigDecimal getOrderTotalAmount(String dt);


    //查询分时金额
    public List<HourAmount> getOrderHourAmount(String dt);

}
