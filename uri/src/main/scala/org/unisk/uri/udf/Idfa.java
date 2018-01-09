package org.unisk.uri.udf;

/**
 * @author sunyunjie (jaysunyun_361@163.com)
 * @date 08/11/2017
 */
public class Idfa {
    public String get_idfa(String log) {

        if (log == null || log.length() <= 0) {
            return "";
        }

        String s = log.toLowerCase();

        if (s.contains("idfa=")) {
            log = log.substring(s.indexOf("idfa=") + "idfa=".length());
            if (log.length() > 31) {
                int i = 0;
                for (; i < log.length(); i++) {
                    if (!Character.isLetterOrDigit(log.charAt(i))
                            && !isLine(log.charAt(i))) {
                        break;
                    }
                }
                if (i != 0) {
                    log = log.substring(0, i);
                    if (log.length() == 32 || log.length() == 36) {
                        return log;
                    }
                }
            }
        }
        else if (s.contains("track.adsage.com") && s.contains("uid=")) {
            log = log.substring(s.indexOf("uid=") + "uid=".length());
            if (log.length() > 31) {
                int i = 0;
                for (; i < log.length(); i++) {
                    if (!Character.isLetterOrDigit(log.charAt(i))
                            && !isLine(log.charAt(i))) {
                        break;
                    }
                }
                if (i != 0) {
                    log = log.substring(0, i);
                    if (log.length() == 32 || log.length() == 36) {
                        return log;
                    }
                }
            }
        }
        else if (s.contains("adfill.adview.cn") && s.contains("ud=")) {
            log = log.substring(s.indexOf("ud=") + "ud=".length());
            if (log.length() > 31) {
                int i = 0;
                for (; i < log.length(); i++) {
                    if (!Character.isLetterOrDigit(log.charAt(i))
                            && !isLine(log.charAt(i))) {
                        break;
                    }
                }
                if (i != 0) {
                    log = log.substring(0, i);
                    if (log.length() == 32 || log.length() == 36) {
                        return log;
                    }
                }
            }
        }
        else if (s.contains("config.adview.cn") && s.contains("uuid=")) {
            log = log.substring(s.indexOf("uuid=") + "uuid=".length());
            if (log.length() > 31) {
                int i = 0;
                for (; i < log.length(); i++) {
                    if (!Character.isLetterOrDigit(log.charAt(i))
                            && !isLine(log.charAt(i))) {
                        break;
                    }
                }
                if (i != 0) {
                    log = log.substring(0, i);
                    if (log.length() == 32 || log.length() == 36) {
                        return log;
                    }
                }
            }
        }
        else if (s.contains("api.mix.guohead.com") && s.contains("adid=")) {
            log = log.substring(s.indexOf("adid=") + "adid=".length());
            if (log.length() > 31) {
                int i = 0;
                for (; i < log.length(); i++) {
                    if (!Character.isLetterOrDigit(log.charAt(i))
                            && !isLine(log.charAt(i))) {
                        break;
                    }
                }
                if (i != 0) {
                    log = log.substring(0, i);
                    if (log.length() == 32 || log.length() == 36) {
                        return log;
                    }
                }
            }
        }
        else if (s.contains("w.m.taobao.com") && s.contains("aid=")) {
            log = log.substring(s.indexOf("aid=") + "aid=".length());
            if (log.length() > 31) {
                int i = 0;
                for (; i < log.length(); i++) {
                    if (!Character.isLetterOrDigit(log.charAt(i))
                            && !isLine(log.charAt(i))) {
                        break;
                    }
                }
                if (i != 0) {
                    log = log.substring(0, i);
                    if (log.length() == 32 || log.length() == 36) {
                        return log;
                    }
                }
            }
        }
        else if (s.contains("vgauto.video.qq.com")
                && s.contains("%26idfa%3D")) {
            log = log
                    .substring(s.indexOf("%26idfa%3D") + "%26idfa%3D".length());
            if (log.length() > 31) {
                int i = 0;
                for (; i < log.length(); i++) {
                    if (!Character.isLetterOrDigit(log.charAt(i))
                            && !isLine(log.charAt(i))) {
                        break;
                    }
                }
                if (i != 0) {
                    log = log.substring(0, i);
                    if (log.length() == 32 || log.length() == 36) {
                        return log;
                    }
                }
            }
        }
        else if (s.contains("vgauto.video.qq.com") && s.contains("a=")) {
            log = log.substring(s.indexOf("a=") + "a=".length());
            if (log.length() > 31) {
                int i = 0;
                for (; i < log.length(); i++) {
                    if (!Character.isLetterOrDigit(log.charAt(i))
                            && !isLine(log.charAt(i))) {
                        break;
                    }
                }
                if (i != 0) {
                    log = log.substring(0, i);
                    if (log.length() == 32 || log.length() == 36) {
                        return log;
                    }
                }
            }
        }
        else if (s.contains("val.atm.youku.com") && s.contains("ck=")) {
            log = log.substring(s.indexOf("ck=") + "ck=".length());
            if (log.length() > 31) {
                int i = 0;
                for (; i < log.length(); i++) {
                    if (!Character.isLetterOrDigit(log.charAt(i))
                            && !isLine(log.charAt(i))) {
                        break;
                    }
                }
                if (i != 0) {
                    log = log.substring(0, i);
                    if (log.length() == 32 || log.length() == 36) {
                        return log;
                    }
                }
            }
        }
        return "";
    }

    public static boolean isLine(char c) {
        if (c == '-') {
            return true;
        }
        return false;
    }

    public static boolean isDigit(String s) {
        if (s == null || s.length() == 0) {
            return false;
        }

        for (int i = 0; i < s.length(); i++) {
            if (!Character.isDigit(s.charAt(i))) {
                return false;
            }
        }
        return true;
    }

}
