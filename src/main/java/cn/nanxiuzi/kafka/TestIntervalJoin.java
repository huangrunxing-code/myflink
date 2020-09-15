package cn.nanxiuzi.kafka;

import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;

/**
 * 功能描述:
 *
 * @ClassName: TestIntervalJoin
 * @Author: huangrx丶
 * @Date: 2020/7/7 11:48
 * @Version: V1.0
 */
public class TestIntervalJoin {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment ev = StreamExecutionEnvironment.getExecutionEnvironment();
        ev.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        DataStream<Transcript> tds = ev.fromElements(TRANSCRIPTS).assignTimestampsAndWatermarks(new AscendingTimestampExtractor<Transcript>() {
            @Override
            public long extractAscendingTimestamp(Transcript element) {
                return element.time;
            }
        });
        DataStream<Student> sds = ev.fromElements(STUDENTS).assignTimestampsAndWatermarks(new AscendingTimestampExtractor<Student>() {
            @Override
            public long extractAscendingTimestamp(Student element) {

                return element.time;
            }
        });
        KeyedStream<Transcript, String> transcriptKeyrdStream = tds.keyBy((Transcript t)->{return t.id;});
       /* KeyedStream<Transcript, String> transcriptKeyrdStream = tds.keyBy(new KeySelector<Transcript, String>() {
            @Override
            public String getKey(Transcript transcript) throws Exception {
                return transcript.id;
            }
        });*/

        KeyedStream<Student, String> studentStringKeyedStream = sds.keyBy((Student::getId));
        sds.print();
        ev.execute("sd");
    }

    public static final Transcript[] TRANSCRIPTS=new Transcript[]{
            new Transcript("1","黄一","语文",100,System.currentTimeMillis()),
            new Transcript("2","赵四","语文",70,System.currentTimeMillis()),
            new Transcript("3","刘能","语文",80,System.currentTimeMillis()),
            new Transcript("4","胡集","语文",55,System.currentTimeMillis()),
            new Transcript("5","陈世杰","语文",76,System.currentTimeMillis()),
            new Transcript("6","刘教授","语文",67,System.currentTimeMillis()),
    } ;
    public static final Student[] STUDENTS=new Student[]{
            new Student("1","黄一","class1",System.currentTimeMillis()),
            new Student("2","赵四","class1",System.currentTimeMillis()),
            new Student("3","刘能","class1",System.currentTimeMillis()),
            new Student("4","胡集","class2",System.currentTimeMillis()),
            new Student("5","陈世杰","class2",System.currentTimeMillis()),
            new Student("6","刘教授","class2",System.currentTimeMillis())
    };


    private static class Transcript{
        private String id;
        private String name;
        private String subject;
        private int score;
        private long time;

        public Transcript(String id, String name, String subject, int score, long time) {
            this.id = id;
            this.name = name;
            this.subject = subject;
            this.score = score;
            this.time = time;
        }

        public String getId() {
            return id;
        }

        public String getName() {
            return name;
        }

        public String getSubject() {
            return subject;
        }

        public int getScore() {
            return score;
        }

        public long getTime() {
            return time;
        }

        public void setId(String id) {
            this.id = id;
        }

        public void setName(String name) {
            this.name = name;
        }

        public void setSubject(String subject) {
            this.subject = subject;
        }

        public void setScore(int score) {
            this.score = score;
        }

        public void setTime(long time) {
            this.time = time;
        }

        @Override
        public String toString() {
            return "Transcript{" +
                    "id='" + id + '\'' +
                    ", name='" + name + '\'' +
                    ", subject='" + subject + '\'' +
                    ", score=" + score +
                    ", time=" + time +
                    '}';
        }
    }
    private static class Student{
        private String id;
        private String name;
        private String classes;
        private long time;

        public Student(){

        }

        public Student(String id, String name, String classes, long time) {
            this.id = id;
            this.name = name;
            this.classes = classes;
            this.time = time;
        }

        public String getId() {
            return id;
        }

        public String getName() {
            return name;
        }

        public String getClasses() {
            return classes;
        }

        public long getTime() {
            return time;
        }

        public void setId(String id) {
            this.id = id;
        }

        public void setName(String name) {
            this.name = name;
        }

        public void setClasses(String classes) {
            this.classes = classes;
        }

        public void setTime(long time) {
            this.time = time;
        }

        @Override
        public String toString() {
            return "Student{" +
                    "id='" + id + '\'' +
                    ", name='" + name + '\'' +
                    ", classes='" + classes + '\'' +
                    ", time=" + time +
                    '}';
        }
    }


}
