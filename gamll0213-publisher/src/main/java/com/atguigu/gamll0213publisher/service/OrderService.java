package com.atguigu.gamll0213publisher.service;

import org.apache.ibatis.annotations.Param;

import java.math.BigDecimal;
import java.util.List;
import java.util.Map;

/**
 * @author lcy
 * @create 2020-07-31 19:26
 */
public interface OrderService {

    //查询当日交易总额
    public BigDecimal getOrderTotalAmount(String dt);

    //查询当日交易额分时明细
    public Map getOrderHourAmount(String dt);

    //查询品牌总额
    public List<Map> getTrademarkSum( String startDt, String endDt);

    //查询省份总额
    public List<Map> getProvinceSum(String startDt,String endDt);
}
