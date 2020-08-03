package com.atguigu.gamll0213publisher.controller;

import com.alibaba.fastjson.JSON;
import com.atguigu.gamll0213publisher.service.DauService;
import com.atguigu.gamll0213publisher.service.OrderService;
import org.apache.commons.lang3.time.DateUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.math.BigDecimal;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * @author lcy
 * @create 2020-07-23 19:19
 */

@RestController
public class PublisherController {

    @Autowired
    DauService dauService;
    @Autowired
    OrderService orderService;

    @RequestMapping("realtime-total")
    public String realtimeTotal(@RequestParam("date") String date){

        List<Map<String,Object>>  totalList = new ArrayList<>();
        Map<String,Object> dauMap = new HashMap<>();
        dauMap.put("id","dau");
        dauMap.put("name","新增日活");
        Long dauTotal = dauService.getDauTotal(date);
        dauMap.put("value",dauTotal);
        totalList.add(dauMap);

        Map<String,Object> newMap = new HashMap<>();
        newMap.put("id","new_mid");
        newMap.put("name","新增设备");
        newMap.put("value",233);
        totalList.add(newMap);

        Map<String,Object> orderMap = new HashMap<>();
        orderMap.put("id","order_amount");
        orderMap.put("name","新增交易额");
        BigDecimal orderTotalAmount = orderService.getOrderTotalAmount(date);
        orderMap.put("value",orderTotalAmount);
        totalList.add(orderMap);

        return JSON.toJSONString(totalList);
    }

    @RequestMapping("realtime-hour")
    public String realtimeHour(@RequestParam("date") String date,@RequestParam("id") String id){

        //判断id是否等dau ，等于则表示处理的是日活数据
        if("dau".equals(id)) {
            Map dauHourCountToday = dauService. getDauHour(date);
            String yd = getYd(date);
            Map dauHourCountYesterday = dauService.getDauHour(yd);

            Map<String, Map<String, Long>> hourCountMap = new HashMap<>();

            hourCountMap.put("today", dauHourCountToday);
            hourCountMap.put("yesterday", dauHourCountYesterday);
            return JSON.toJSONString(hourCountMap);
        }else if("order_amount".equals(id)) {
            Map orderHourAmountToday = orderService.getOrderHourAmount(date);
            String yd = getYd(date);
            Map orderHourAmountYesterday = orderService.getOrderHourAmount(yd);

            Map hourAmountMap = new HashMap();
            hourAmountMap.put("yesterday",orderHourAmountYesterday);
            hourAmountMap.put("today",orderHourAmountToday);

            return JSON.toJSONString(hourAmountMap);
        }else {
            return  null;
        }
    }

    /**
     * 根据当前日期求出昨天的日期的字符串格式
     * @param td
     * @return
     */
    private String getYd(String td){
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");

        try {
            Date tdDate = simpleDateFormat.parse(td);
            Date ydDate = DateUtils.addDays(tdDate, -1);
            return simpleDateFormat.format(ydDate);
        } catch (ParseException e) {
            throw new RuntimeException("格式转换有误");
        }
    }
}
