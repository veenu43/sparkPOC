package spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.rdd.MapPartitionsRDD;
import scala.Tuple2;

import java.util.Arrays;

public class WordCounterPlural {

    private static void wordCount() {

        SparkConf sparkConf = new SparkConf().setMaster("local").setAppName("JD Word Counter");

        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);

        JavaRDD<String> inputFile = sparkContext.textFile("D:\\Spark\\spark-2.4.5-bin-hadoop2.7\\README.md");


        JavaRDD<String> tokenizedFileData = inputFile.flatMap(content -> Arrays.asList(content.split(" ")).iterator());

        JavaPairRDD<String,Integer> countPrep = tokenizedFileData.mapToPair(word -> new Tuple2(word, 1));
        JavaPairRDD<String,Integer> counts = countPrep.reduceByKey((x, y) -> (int) x + (int) y);
        JavaPairRDD<String,Integer> sortedCount = counts.sortByKey();
        sortedCount.saveAsTextFile("ReadmeCount");
    }

    public static void main(String[] args) {
       wordCount();
    }
}
