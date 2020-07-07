package cn.richinfo.javabaseknowledge;

import java.util.Random;

/**
 * 功能描述:
 *
 * @ClassName: Romdon
 * @Author: huangrx丶
 * @Date: 2020/6/22 20:28
 * @Version: V1.0
 */
public class Romdon {
    public static void main(String[] args) throws InterruptedException {
        while(true) {
            System.out.println(new Random().nextInt(100));
            Thread.sleep(500);
        }
    }
}
