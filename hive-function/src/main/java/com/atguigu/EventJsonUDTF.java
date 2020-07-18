package com.atguigu;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.ql.exec.MapredContext;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import java.util.ArrayList;
import java.util.List;

/**
 * @Auther wu
 * @Date 2019/7/30  21:20
 */
public class EventJsonUDTF extends GenericUDTF {

    //指定输出的参数的类型和名称
    @Override
    public StructObjectInspector initialize(StructObjectInspector argOIs) throws UDFArgumentException {

        //返回值-->列名
        List<String> fieldNames = new ArrayList<String>();
        fieldNames.add("event_name");
        fieldNames.add("event_json");

        //返回值-->列的类型
        List<ObjectInspector> fieldOIs = new ArrayList<ObjectInspector>();
        fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
        fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);

        return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames, fieldOIs);
    }

    @Override
    public void process(Object[] args) throws HiveException {

        //获取数据格式[{事件1},{事件2},{事件...}]
        String input = args[0].toString();

        //判断数据是否为空
        if (StringUtils.isBlank(input)) {
            return;
        }

        try {

            //创建input数据的json数组
            JSONArray jsonArray = new JSONArray(input);

            //循环遍历每一个事件
            for (int i = 0; i < jsonArray.length(); i++) {

                //创建输出数据的数组
                String[] strings = new String[2];

                //获取事件名称
                JSONObject eventJsonObj = jsonArray.getJSONObject(i);
                String eventName = eventJsonObj.getString("en");
                strings[0] = eventName;

                //获取事件整体
                strings[1] = eventJsonObj.toString();

                //写出
                forward(strings);
            }
        } catch (JSONException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void close() throws HiveException {

    }
}
