package com.atguigu.gamll0213publisher.mapper;

import org.apache.ibatis.annotations.Param;

import java.util.List;
import java.util.Map;

/**
 * @author lcy
 * @create 2020-08-02 22:44
 */
public interface TrademarkStatMapper {

    public List<Map> getTrademarkSum(@Param("startDt") String startDt ,@Param("endDt") String endDt);
}
