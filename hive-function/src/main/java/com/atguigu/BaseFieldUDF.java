package com.atguigu;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.tools.ant.Evaluable;
import org.json.JSONException;
import org.json.JSONObject;

/**
 * @Auther wu
 * @Date 2019/7/30  20:43
 */
public class BaseFieldUDF extends UDF {

    public String evaluate(String line, String cmKeys) {

        //创建StringBuilder用于拼接字符串
        StringBuffer sb = new StringBuffer();
        try {
            //切割cmKeys
            String[] keys = cmKeys.split(",");

            //切割原始数据
            String[] fields = line.split("\\|");
            String dt = fields[0];

            //获取fields[1]对应json对象
            JSONObject jsonObject = new JSONObject(fields[1]);

            //取出公共字段的json对象
            JSONObject cmJsonObj = jsonObject.getJSONObject("cm");
            for (String key : keys) {

                //判断key是否存在
                if (cmJsonObj.has(key)) {
                    String value = cmJsonObj.getString(key);
                    sb.append(value).append("\t");
                } else {
                    sb.append("\t");
                }
            }

            //拼接事件数组
            sb.append(jsonObject.getString("et")).append("\t");

            //拼接时间戳
            sb.append(fields[0]);
        } catch (JSONException e) {
            e.printStackTrace();
        }
        return sb.toString();
    }
}