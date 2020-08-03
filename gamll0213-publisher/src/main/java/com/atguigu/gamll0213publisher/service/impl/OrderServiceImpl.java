package com.atguigu.gamll0213publisher.service.impl;

import com.atguigu.gamll0213publisher.bean.HourAmount;
import com.atguigu.gamll0213publisher.mapper.OrderWideMapper;
import com.atguigu.gamll0213publisher.mapper.ProvinceStatMapper;
import com.atguigu.gamll0213publisher.mapper.TrademarkStatMapper;
import com.atguigu.gamll0213publisher.service.OrderService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


/**
 * @author lcy
 * @create 2020-07-31 19:29
 */

@Service
public class OrderServiceImpl implements OrderService {

    @Autowired
    OrderWideMapper orderWideMapper;

    @Autowired
    TrademarkStatMapper trademarkStatMapper;

    @Autowired
    ProvinceStatMapper provinceStatMapper;

    @Override
    public BigDecimal getOrderTotalAmount(String dt) {
        return orderWideMapper.getOrderTotalAmount(dt);
    }

    @Override
    public Map getOrderHourAmount(String dt) {

        List<HourAmount> hourAmountList = orderWideMapper.getOrderHourAmount(dt);
        Map hourAmountMap = new HashMap();
        for (HourAmount hourAmount : hourAmountList) {
            hourAmountMap.put(hourAmount.getHr(),hourAmount.getOrderAmount());
        }

        return hourAmountMap;
    }

    @Override
    public List<Map> getTrademarkSum(String startDt, String endDt) {
        return trademarkStatMapper.getTrademarkSum(startDt,endDt);
    }

    @Override
    public List<Map> getProvinceSum(String startDt, String endDt) {
        return provinceStatMapper.getProvinceSum(startDt,endDt);
    }
}
