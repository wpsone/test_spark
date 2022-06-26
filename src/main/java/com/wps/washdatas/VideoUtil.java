package com.wps.washdatas;

public class VideoUtil {

    public static String washDatas(String line) {
        if (null == line || "".equals(line)) {
            return null;
        }

        //判断数据的长度，如果小于9，直接丢掉
        String[] split = line.split("\t");
        if (split.length < 9) {
            return null;
        }

        //将视频类别空格进行去掉
        split[3] = split[3].replace(" ", "");
        StringBuilder builder = new StringBuilder();
        for (int i = 0; i < split.length; i++) {
            if (i<9) {
                //这里面是前面八个字段
                builder.append(split[i]).append("\t");
            } else if (i>=9 && i<split.length-1) {
                builder.append(split[i]).append("&");
            } else if (i==split.length-1) {
                builder.append(split[i]);
            }
        }
        return builder.toString();
    }
}
