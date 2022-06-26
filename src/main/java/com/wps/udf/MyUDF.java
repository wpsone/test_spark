package com.wps.udf;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

public class MyUDF extends UDF {

    //定义udf函数，将小写转换成为大写
    public Text evaluate(String param) {
        if (null != param && !param.equals("")) {
            String s = param.toUpperCase();
            return new Text(s);
        }
        return null;
    }
}
