package it.polito.s256654.thesis.algorithm;

import java.io.Serializable;

public abstract class OutlierDetector implements Serializable {

    private static final long serialVersionUID = 1L;
    
    protected int dim;
    protected double eps;
    protected int minPts;

    /**
     * Runs the algorithm, coordinating the execution of the different tasks.
     * 
     * @param inputPath The path of the input file.
     * @param outputPath The path of the output file.
     * @param printStats Print statistics to the standard output.
     */
    public abstract void run(String inputPath, String outputPath, boolean printStats);

}