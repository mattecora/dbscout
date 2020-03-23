package it.polito.s256654.thesis;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;

import it.polito.s256654.thesis.algorithm.OutlierDetector;
import it.polito.s256654.thesis.algorithm.parallel.ParallelOutlierDetector;

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
        boolean stats = cmd.hasOption("stats");
        int numPart = Integer.parseInt(cmd.getOptionValue("numPart", "0"));

        /* Get the start time */
        long startTime = System.currentTimeMillis();

        try {
            /* Instantiate the algorithm */
            OutlierDetector od = (OutlierDetector) Class.forName(cmd.getOptionValue("algClass"))
                .getConstructor(int.class, double.class, int.class)
                .newInstance(dim, eps, minPts);

            /* Run the algorithm */
            if (numPart != 0)
                ((ParallelOutlierDetector) od).run(inputPath, outputPath, stats, numPart);
            else
                od.run(inputPath, outputPath, cmd.hasOption("stats"));
        } catch (Exception e) {
            e.printStackTrace();
        }

        /* Print the execution time */
        System.out.println("Execution time: " + ((System.currentTimeMillis() - startTime) / 1000.0) + " seconds");
    }

    private static CommandLine parseCLI(String[] args) {
        /* Instantiate CLI options */
        Options options = new Options();

        /* Algorithm parameter */
        Option algClassParam = new Option(null, "algClass", true, "The algorithm class");
        algClassParam.setRequired(true);
        options.addOption(algClassParam);

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

        /* Partitions parameter */
        Option numPartParam = new Option(null, "numPart", true, "The number of partitions");
        options.addOption(numPartParam);

        /* Stats parameter */
        Option statsParam = new Option(null, "stats", false, "Print dataset statistics");
        options.addOption(statsParam);

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
