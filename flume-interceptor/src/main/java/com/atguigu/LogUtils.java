package com.atguigu;

import org.apache.commons.lang.math.NumberUtils;

/**
 * @Auther wu
 * @Date 2019/7/28  22:20
 */
public class LogUtils {

    //过滤启动日志
    public static boolean valiType(String body) {
        return body.startsWith("{") && body.endsWith("}");
    }

    //过滤事件日志
    public static boolean valiEvent(String body) {

        String[] fields = body.split("\\|");

        if(fields.length!=2){
            return false;
        }

        if(!NumberUtils.isDigits(fields[0])){
            return false;
        }

        return fields[1].startsWith("{") && fields[1].endsWith("}");
    }
}
