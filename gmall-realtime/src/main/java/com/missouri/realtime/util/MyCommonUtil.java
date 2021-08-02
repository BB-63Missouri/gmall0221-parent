package com.missouri.realtime.util;

import java.util.ArrayList;
import java.util.List;

/**
 * @author Missouri
 * @date 2021/7/28 18:09
 */
//数据放到list工具类
public class MyCommonUtil {
    public static <T>List<T> tolist(Iterable<T> iterable){
        List<T> result = new ArrayList<>();
        for (T t : iterable) {
            result.add(t);
        }
        return result;
    }
}
