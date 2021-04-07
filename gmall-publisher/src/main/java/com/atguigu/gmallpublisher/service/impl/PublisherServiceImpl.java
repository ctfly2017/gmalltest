package com.atguigu.gmallpublisher.service.impl;

import com.atguigu.gmallpublisher.mapper.DauMapper;
import com.atguigu.gmallpublisher.service.PublisherService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.List;
import java.util.Map;


@Service
public class PublisherServiceImpl implements PublisherService {


    @Autowired
    private DauMapper dauMapper;

    @Override
    public Integer getDAUTotal(String date) {
        return null;
    }

    @Override
    public Map<String, Long> getDauTotalHourMap(String date) {
        //从
        List<Map> list = dauMapper.selectDauTotalHourMap(date);

        HashMap<String,Long> result = new HashMap<>();

        for (Map map : list) {
            result.put((String) map.get("LH"), (Long) map.get("CT"));
        }
        return null;
    }
}
