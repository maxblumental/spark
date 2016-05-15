package ru.mipt;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;

public class IpAddressAnalyzer {
    public static void main(String[] args) {
        if (args.length != 3)
            throw new IllegalArgumentException("Not enough arguments!");

        String input = args[0];
        String output = args[1];
        String master = args[2];

        SparkConf sparkConf = new SparkConf().setAppName("IP with 7 counter").setMaster(master);
        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);

        List<String> top = sparkContext.textFile(input)
                .filter(new Function<String, Boolean>() {
                    public Boolean call(String v1) throws Exception {
                        return v1.split("\\s+")[0].contains("7");
                    }
                })
                .mapToPair(new PairFunction<String, String, Integer>() {
                    public Tuple2<String, Integer> call(String s) throws Exception {
                        return new Tuple2<>(s.split("\\s+")[0], 1);
                    }
                })
                .reduceByKey(new Function2<Integer, Integer, Integer>() {
                    @Override
                    public Integer call(Integer v1, Integer v2) throws Exception {
                        return v1 + v2;
                    }
                })
                .map(new Function<Tuple2<String, Integer>, Tuple2<Integer, String>>() {
                    @Override
                    public Tuple2<Integer, String> call(Tuple2<String, Integer> v1) throws Exception {
                        return v1.swap();
                    }
                })
                .sortBy(new Function<Tuple2<Integer, String>, Integer>() {
                    @Override
                    public Integer call(Tuple2<Integer, String> v1) throws Exception {
                        return v1._1;
                    }
                }, false, 1)
                .map(new Function<Tuple2<Integer, String>, String>() {
                    @Override
                    public String call(Tuple2<Integer, String> v1) throws Exception {
                        return String.format("%s %d", v1._2, v1._1);
                    }
                })
                .take(10);

        try (BufferedWriter bufferedWriter = Files.newBufferedWriter(Paths.get(output), Charset.defaultCharset())) {
            for (String s : top) {
                bufferedWriter.write(s + "\n");
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            sparkContext.stop();
        }
    }
}
