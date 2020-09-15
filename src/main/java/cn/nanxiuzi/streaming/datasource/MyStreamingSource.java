package cn.nanxiuzi.streaming.datasource;

import com.github.javafaker.Faker;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Date;
import java.util.Locale;

/**
 * 功能描述:
 *
 * @ClassName: MyStreamingSource
 * @Author: huangrx丶
 * @Date: 2020/9/11 23:03
 * @Version: V1.0
 */
public class MyStreamingSource implements SourceFunction<String> {
    private boolean isRunable=true;
    @Override
    public void run(SourceContext<String> sourceContext) throws Exception {
        while(isRunable){
            StringBuilder sb = new StringBuilder();
            Faker faker = new Faker(new Locale("zh-CN"));
            sb.append(faker.random().nextInt(10000)).append("|")
            .append(faker.name().name()).append("|")
            .append(faker.address().fullAddress()).append("|")
            .append(faker.phoneNumber().cellPhone()).append("|")
            .append(faker.demographic().sex()).append("|")
            .append(new Date().toString());
            sourceContext.collect(sb.toString());
            Thread.sleep(1000);
        }
    }

    @Override
    public void cancel() {
        isRunable=false;
    }

}
