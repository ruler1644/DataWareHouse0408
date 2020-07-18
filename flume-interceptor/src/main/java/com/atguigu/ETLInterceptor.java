package com.atguigu;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;

import java.util.ArrayList;
import java.util.List;

/**
 * @Auther wu
 * @Date 2019/7/28  22:11
 */
public class ETLInterceptor implements Interceptor {

    private List<Event> interceptors = new ArrayList<Event>();

    public void initialize() {

    }

    public Event intercept(Event event) {
        String body = new String(event.getBody());
        boolean flag ;
        if(body.contains("start")){
            flag = LogUtils.valiType(body);
        }else{
            flag = LogUtils.valiEvent(body);
        }

        //据返回值，决定是否过滤数据
        if(flag){
            return event;
        }else{
            return null;
        }
    }

    public List<Event> intercept(List<Event> events) {

        //清空集合
        interceptors.clear();

        for (Event event : events) {

            //过滤事件
            Event intercept = intercept(event);

            //判断当前事件是否为空
            if(event!=null){
                interceptors.add(event);
            }
        }
        return interceptors;
    }

    public void close() {

    }

    public static class Build implements Interceptor.Builder{

        public Interceptor build() {
            return new ETLInterceptor();
        }

        public void configure(Context context) {

        }
    }
}
