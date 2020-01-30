package it.polito.s256654.thesis;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

public class App {

    public static void main(String[] args) {
        /* Parse the command line */
        CommandLine cmd = parseCLI(args);

        /* Get input parameters */
        String inputPath = cmd.getOptionValue("inputPath");
        String outputPath = cmd.getOptionValue("outputPath");
        int dim = Integer.parseInt(cmd.getOptionValue("dim"));
        double eps = Double.parseDouble(cmd.getOptionValue("eps"));
        int minPts = Integer.parseInt(cmd.getOptionValue("minPts"));

        /* Get the start time */
        long startTime = System.currentTimeMillis();

        /* Define the Spark context */
        SparkConf conf = new SparkConf().setAppName("Outlier detector");
        JavaSparkContext sc = new JavaSparkContext(conf);

        /* Instantiate the algorithm */
        OutlierDetector od = new OutlierDetector(sc, dim, eps, minPts);

        /* Run the algorithm */
        od.run(inputPath, outputPath, cmd.hasOption("stats"), cmd.hasOption("bjoin"));

        /* Close the Spark context */
        sc.close();

        /* Print the execution time */
        System.out.println("Execution time: " + ((System.currentTimeMillis() - startTime) / 1000.0) + " seconds");
    }

    private static CommandLine parseCLI(String[] args) {
        /* Instantiate CLI options */
        Options options = new Options();

        /* Input file parameter */
        Option inputPathParam = new Option(null, "inputPath", true, "The input path");
        inputPathParam.setRequired(true);
        options.addOption(inputPathParam);

        /* Output path parameter */
        Option outputPathParam = new Option(null, "outputPath", true, "The output path");
        outputPathParam.setRequired(true);
        options.addOption(outputPathParam);

        /* Dim parameter */
        Option dimParam = new Option(null, "dim", true, "The data dimensions");
        dimParam.setRequired(true);
        options.addOption(dimParam);

        /* Eps parameter */
        Option epsParam = new Option(null, "eps", true, "The eps value");
        epsParam.setRequired(true);
        options.addOption(epsParam);

        /* MinPts parameter */
        Option minPtsParam = new Option(null, "minPts", true, "The minPts value");
        minPtsParam.setRequired(true);
        options.addOption(minPtsParam);

        /* Stats parameter */
        Option statsParam = new Option(null, "stats", false, "Only show dataset statistics");
        options.addOption(statsParam);

        /* Broadcast join parameter */
        Option bjoinParam = new Option(null, "bjoin", false, "Use broadcast join");
        options.addOption(bjoinParam);

        /* Read command line */
        CommandLineParser parser = new PosixParser();
        HelpFormatter formatter = new HelpFormatter();
        CommandLine cmd = null;

        try {
            cmd = parser.parse(options, args);
        } catch (ParseException e) {
            System.out.println(e.getMessage());
            formatter.printHelp("spark-submit --class it.polito.s256654.thesis.App thesis-code.jar", options);
            System.exit(1);
        }

        return cmd;
    }

}
