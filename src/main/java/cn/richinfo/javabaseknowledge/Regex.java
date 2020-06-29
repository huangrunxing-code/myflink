package cn.richinfo.javabaseknowledge;

import java.util.Arrays;
import java.util.HashSet;

public class Regex {
    public static void main(String[] args) {
        String s="[0-9]";
        System.out.println("s".matches(s));
        HashSet<String> fieldSet = new HashSet<>();
        fieldSet.add("aaa");
        fieldSet.add("bbb");
        fieldSet.add("ccc");
        fieldSet.add("ddd");
        System.out.println(Arrays.toString(fieldSet.toArray()).replaceAll("\\[","").replaceAll("]",""));
    }
}
