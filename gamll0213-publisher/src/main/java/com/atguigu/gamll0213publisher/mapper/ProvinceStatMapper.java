package com.atguigu.gamll0213publisher.mapper;

import org.apache.ibatis.annotations.Param;

import java.util.List;
import java.util.Map;

/**
 * @author lcy
 * @create 2020-08-03 21:23
 */
public interface ProvinceStatMapper {

    public List<Map> getProvinceSum (@Param("startDt") String startDt , @Param("endDt") String endDt);
}
