package it.polito.s256654.thesis;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

public class OutlierDetector implements Serializable {
    
    private static final long serialVersionUID = 1L;
    
    private int dim;
    private double eps;
    private int minPts;

    public OutlierDetector(int dim, double eps, int minPts) {
        this.dim = dim;
        this.eps = eps;
        this.minPts = minPts;
    }

    /**
     * Runs the algorithm, coordinating the execution of the different tasks.
     * 
     * @param inputPath The path of the input file.
     * @param outputPath The path of the output file.
     */
    public void run(String inputPath, String outputPath) {
        /* Create the grid */
        Map<Cell, List<Vector>> allPoints = parseInputAndCreateGrid(inputPath);

        /* Get core points */
        Map<Cell, List<Vector>> corePoints = findCorePoints(allPoints);

        /* Get outliers */
        Map<Cell, List<Vector>> outliers = findOutliers(allPoints, corePoints);

        /* Save output file */
        saveResults(outliers, outputPath);
    }

    /**
     * Parses the input vectors and constructs the grid of points with diagonal eps.
     * 
     * @param inputPath The path of the file to be parsed.
     * @return A Map containing, for all cells, the corresponding points.
     */
    private Map<Cell, List<Vector>> parseInputAndCreateGrid(String inputPath) {
        Map<Cell, List<Vector>> allPoints = new HashMap<>();

        /* Open the file for reading */
        try (BufferedReader br = new BufferedReader(new FileReader(inputPath))) {
            String line;

            while ((line = br.readLine()) != null) {
                /* Remove header lines */
                if (line.startsWith("x"))
                    continue;
                
                String[] tokens = line.split(",");
                double[] coords = new double[dim];
                int[] pos = new int[dim];

                /* Parse vector and compute cell coordinates */
                for (int i = 0; i < dim; i++) {
                    coords[i] = Double.parseDouble(tokens[i]);
                    pos[i] = (int) (coords[i] / eps * Math.sqrt(dim));
                }

                /* Construct the cell and vector */
                Cell c = new Cell(pos);
                Vector v = new Vector(coords);

                /* Insert the point in the map */
                if (!allPoints.containsKey(c))
                    allPoints.put(c, new ArrayList<>());
                allPoints.get(c).add(v);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        return allPoints;
    }

    /**
     * Returns the core points for each cell.
     * 
     * @param allPoints The map representing the input vectors.
     * @return A map containing, for all cells, its core points.
     */
    private Map<Cell, List<Vector>> findCorePoints(Map<Cell, List<Vector>> allPoints) {
        Map<Cell, List<Vector>> corePoints = new HashMap<>();

        for (Entry<Cell, List<Vector>> e : allPoints.entrySet()) {
            if (e.getValue().size() >= minPts) {
                /* Dense cell, all points are core points */
                corePoints.put(e.getKey(), e.getValue());
            } else {
                /* Non-dense cell, check points in neighboring cells */
                List<Cell> neighbors = e.getKey().generateNeighbors();
                
                /* For all vectors in the current cell ... */
                for (Vector v1 : e.getValue()) {
                    long countNeighbors = 0;

                    /* ... check all the neighboring cells ... */
                    for (Cell n : neighbors) {
                        /* ... if they contain at least a point ... */
                        if (allPoints.containsKey(n)) {
                            /* ... consider all the vectors in the cell ... */
                            for (Vector v2 : allPoints.get(n)) {
                                /* ... if the distance between the two is less than eps ... */
                                if (v1.distanceTo(v2) < eps) {
                                    /* ... then increment the number of neighbors */
                                    countNeighbors++;
                                }
                            }
                        }
                    }

                    /* Put the point in the map if it has at least minPts neighbors */
                    if (countNeighbors >= minPts) {
                        if (!corePoints.containsKey(e.getKey()))
                            corePoints.put(e.getKey(), new ArrayList<>());
                        corePoints.get(e.getKey()).add(v1);
                    }
                }
            }
        }

        return corePoints;
    }

    /**
     * Returns the outliers for each cell.
     * 
     * @param allPoints The map representing the input vectors.
     * @param corePoints The map representing the core points.
     * @return A map containing, for all cells, its outliers.
     */
    private Map<Cell, List<Vector>> findOutliers(Map<Cell, List<Vector>> allPoints, Map<Cell, List<Vector>> corePoints) {
        Map<Cell, List<Vector>> outliers = new HashMap<>();

        for (Entry<Cell, List<Vector>> e : allPoints.entrySet()) {
            if (!corePoints.containsKey(e.getKey())) {
                /* Non-core cell, check points in neighboring cells */
                List<Cell> neighbors = e.getKey().generateNeighbors();

                /* For all points in the current cell ... */
                for (Vector v1 : e.getValue()) {
                    boolean isOutlier = true;

                    /* ... check all the neighboring core cells ... */
                    for (Cell n : neighbors) {
                        /* ... if they contain at least a point ... */
                        if (corePoints.containsKey(n)) {
                            /* ... consider all the vectors in the cell ... */
                            for (Vector v2 : corePoints.get(n)) {
                                /* ... if the distance between the two is less than eps ... */
                                if (v1.distanceTo(v2) < eps) {
                                    /* ... then the point is not an outlier */
                                    isOutlier = false;
                                }
                            }
                        }
                    }

                    /* Put the point in the map if it has no neighboring core point */
                    if (isOutlier) {
                        if (!outliers.containsKey(e.getKey()))
                            outliers.put(e.getKey(), new ArrayList<>());
                        outliers.get(e.getKey()).add(v1);
                    }
                }
            }
        }

        return outliers;
    }

    /**
     * Save results to a text file.
     * 
     * @param points The map containing the points to be saved.
     * @param outputPath The path of the file to be written.
     */
    private void saveResults(Map<Cell, List<Vector>> points, String outputPath) {
        /* Open the file for writing */
        try (BufferedWriter bw = new BufferedWriter(new FileWriter(outputPath))) {
            /* Write each entry to file */
            for (Entry<Cell, List<Vector>> e : points.entrySet()) {
                for (Vector v : e.getValue())
                    bw.write(v + "\n");
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}