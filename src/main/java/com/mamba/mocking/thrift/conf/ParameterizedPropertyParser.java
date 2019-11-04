package com.mamba.mocking.thrift.conf;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.function.BiFunction;

final class ParameterizedPropertyParser {

    public static <T> T parse(String define, BiFunction<String, Map<String, String>, T> mapper) {
        if (define == null || define.isEmpty()) {
            return null;
        }
        int left = define.indexOf('(');
        if (left < 0) {
            return mapper.apply(trim(define, define.length()), Collections.emptyMap());
        }
        int right = define.lastIndexOf(')');
        if (right < left) {
            throw new IllegalArgumentException();
        }

        int split = 0;
        StringBuilder sb = new StringBuilder();
        Map<String, String> attrs = new HashMap<>();
        for (int i = left + 1, depth = 0; i < right; i++) {
            char ch = define.charAt(i);
            if (ch == ' ') {
                continue;
            }
            if (ch == ',') {
                if (depth == 0) {
                    putAttr(attrs, sb, split);
                    sb.setLength(0);
                    split = 0;
                    continue;
                }
            }
            if (ch == '(') {
                depth++;
            } else if (ch == ')') {
                depth--;
                if (depth < 0) {
                    throw new IllegalArgumentException();
                }
            } else if (ch == '=') {
                if (split == 0) {
                    split = sb.length();
                }
            }
            sb.append(ch);
        }
        if (sb.length() > 0) {
            putAttr(attrs, sb, split);
        }
        return mapper.apply(trim(define, left), attrs);
    }

    private static String trim(String str, int end) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < end; i++) {
            char ch = str.charAt(i);
            if (ch == ' ') {
                continue;
            }
            sb.append(ch);
        }
        return sb.toString();
    }

    private static void putAttr(Map<String, String> attrs, StringBuilder sb, int split) {
        if (split <= 0) {
            throw new IllegalArgumentException();
        }
        attrs.put(sb.substring(0, split), sb.substring(split + 1));
    }
}
