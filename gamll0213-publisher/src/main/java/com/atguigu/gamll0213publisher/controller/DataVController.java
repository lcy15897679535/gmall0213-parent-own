package com.atguigu.gamll0213publisher.controller;

import com.alibaba.fastjson.JSON;
import com.atguigu.gamll0213publisher.service.OrderService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author lcy
 * @create 2020-08-03 15:14
 */
@RestController
public class DataVController {

    @Autowired
    OrderService orderService;

    @GetMapping("/trademark")
    public String getTrademarkStat(@RequestParam("startDt")String startDt, @RequestParam("endDt") String endDt){

        List<Map> trademarkSumList = orderService.getTrademarkSum(startDt, endDt);
        List<Map> rsList = new ArrayList<>();
        for (Map map : trademarkSumList) {
            String tm_name = (String)map.get("tm_name");
            BigDecimal amount = (BigDecimal)map.get("amount");
            HashMap<String, Object> rsMap = new HashMap<>();
            rsMap.put("x",tm_name);
            rsMap.put("y",amount);
            rsMap.put("s","1");
            rsList.add(rsMap);
        }
        return JSON.toJSONString(rsList);
    }


    @GetMapping("/province")
    public String getProvinceStat(@RequestParam("startDt")String startDt, @RequestParam("endDt") String endDt){
        List<Map> provinceSumList = orderService.getProvinceSum(startDt, endDt);
        List<Map> rsList = new ArrayList<>();
        for (Map map : provinceSumList) {
            String province_area_code = (String)map.get("province_area_code");
            BigDecimal amount = (BigDecimal)map.get("sum_amount");
            HashMap<String, Object> rsMap = new HashMap<>();
            rsMap.put("area_id",province_area_code);
            rsMap.put("value",amount);
            rsList.add(rsMap);
        }
        return JSON.toJSONString(rsList);
    }
}
