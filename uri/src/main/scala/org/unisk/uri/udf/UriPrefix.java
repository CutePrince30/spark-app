package org.unisk.uri.udf;

/**
 * @author sunyunjie (jaysunyun_361@163.com)
 * @date 08/11/2017
 */
public class UriPrefix {

    public String get_uri(String log) {
        log = log.trim();
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
