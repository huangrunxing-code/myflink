package cn.richinfo.javabaseknowledge;

/**
 * 功能描述:
 *
 * @ClassName: AboutInteger
 * @Author: huangrx丶
 * @Date: 2020/6/21 22:50
 * @Version: V1.0
 */
public class AboutInteger {
    public static void main(String[] args) {
        //Integer 返回在 -128到127 之间是缓存起来的，大家共享，在这范围之外的会新建对象
        Integer i1=88;
        Integer i2=Integer.valueOf(88);
        Integer i3=888;
        Integer i4=888;
        System.out.println(i1==i2);
        System.out.println(i3==i4);
        System.out.println(i1.equals(i2));
        System.out.println(i3.equals(i4));
    }
}
