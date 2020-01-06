package it.polito.s256654.thesis;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.ml.linalg.Vector;
import org.apache.spark.ml.linalg.Vectors;

public class App {
    public static void main(String[] args) {
        /* Parse input parameters */
        String inputFile = args[0];
        String outputFolder = args[1];
        double eps = Double.parseDouble(args[2]);
        int minPts = Integer.parseInt(args[3]);

        /* Define the Spark context */
        SparkConf conf = new SparkConf().setMaster("local").setAppName("Test");
        JavaSparkContext sc = new JavaSparkContext(conf);

        /* Parse the input file */
        JavaRDD<Vector> points = sc.textFile(inputFile)
            .map(s -> {
                String[] tokens = s.split(",");
                return Vectors.dense(Double.parseDouble(tokens[0]), Double.parseDouble(tokens[1]));
            });
        
        /* Instantiate the algorithm */
        OutlierDetector od = new OutlierDetector(eps, minPts);

        /* Run the algorithm */
        JavaRDD<Vector> outliers = od.run(points);
        
        /* Save results */
        outliers.saveAsTextFile(outputFolder);

        /* Close the Spark context */
        sc.close();
    }

}
