package com.atguigu;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * @Auther wu
 * @Date 2019/7/28  22:11
 */
public class TypeInterceptor implements Interceptor {

    private List<Event> interceptors = new ArrayList<Event>();

    public void initialize() {

    }

    public Event intercept(Event event) {
       
        //获取事件内容
        String body = new String(event.getBody());
        
        //获取事件头
        Map<String, String> headers = event.getHeaders();

        //据事件body添加头信息
        if(body.contains("start")){
            headers.put("topic","topic_start");
        }else{
            headers.put("topic","topic_event");
        }
        return event;
    }

    public List<Event> intercept(List<Event> events) {

        interceptors.clear();

        for (Event event : events) {
            interceptors.add(intercept(event));
        }
        return interceptors;
    }

    public void close() {

    }
    public static class Build implements Interceptor.Builder{

        public Interceptor build() {
            return new TypeInterceptor();
        }

        public void configure(Context context) {

        }
    }
}
