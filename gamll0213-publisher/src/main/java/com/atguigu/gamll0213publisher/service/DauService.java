package com.atguigu.gamll0213publisher.service;

import java.util.Map;

/**
 * @author lcy
 * @create 2020-07-23 19:20
 */
public interface DauService {

    /**
     * 查询日活总数
     * @param date
     * @return
     */
    public Long getDauTotal(String date);


    public Map getDauHour(String date);
}
