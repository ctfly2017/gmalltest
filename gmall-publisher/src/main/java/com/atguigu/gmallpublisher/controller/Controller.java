package com.atguigu.gmallpublisher.controller;


import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmallpublisher.service.PublisherService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

@RestController
public class Controller {

    @Autowired
    private PublisherService publisherService;

    @RequestMapping("realtime-total")
    public String realtimeTotal(@RequestParam("data")String date){
        //从service层获取日活总数据
        Integer dauTotal = publisherService.getDAUTotal(date);

        ArrayList<Map> result = new ArrayList<>();
        //创建存放新增日活的map
        HashMap<String, Object> dauMap = new HashMap<>();
        //创建新增设备的map集合
        HashMap<String, Object> devMap = new HashMap<>();

        dauMap.put("id","dau");
        dauMap.put("name", "新增日活");
        dauMap.put("value", dauTotal);

        devMap.put("id","new_mid");
        devMap.put("name", "新增设备");
        devMap.put("value", 233);

        result.add(dauMap);
        result.add(devMap);

        return JSONObject.toJSONString(result);
    }
    @RequestMapping("realtime-hours")
    public String realtimeHours(@RequestParam("id") String id,
                                @RequestParam("date") String date){
        //获取今天数据
        Map<String, Long> todayHourMap = publisherService.getDauTotalHourMap(date);
       //获取昨天数据
        String yesterday = LocalDate.parse(date).plusDays(-1).toString();

        Map<String, Long> yesterdayHourMap = publisherService.getDauTotalHourMap(yesterday);
        //
        HashMap<String,Object> result = new HashMap<>();
        result.put("yestrday", yesterdayHourMap);
        result.put("today", yesterdayHourMap);

        return JSONObject.toJSONString(result);
    }
}
