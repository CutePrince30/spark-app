package org.unisk.udf;

import org.apache.commons.lang3.StringUtils;

import java.lang.reflect.Array;
import java.util.Arrays;

/**
 * @author sunyunjie (jaysunyun_361@163.com)
 * @date 08/11/2017
 */
public class UriPrefix {

    public String get_uri(String log) {
        if (log.startsWith("http://") || log.startsWith("https://")) {
            return log.split("://")[1];
        }
        return log;
    }

    public static void main(String[] args) {
        String s = "https://www.baidu.com";
        UriPrefix uriPrefix = new UriPrefix();
        System.out.println(uriPrefix.get_uri(s));
    }

}
