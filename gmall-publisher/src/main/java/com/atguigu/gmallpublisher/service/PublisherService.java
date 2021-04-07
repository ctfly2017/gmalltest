package com.atguigu.gmallpublisher.service;

import java.util.Map;

public interface PublisherService {
    //日活总数
    public Integer getDAUTotal(String date);
    //日活分时数据
    public Map<String,Long> getDauTotalHourMap(String date);
}
