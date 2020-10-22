package dbscout.algorithm.sequential;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import dbscout.algorithm.OutlierDetector;
import dbscout.structures.Cell;
import dbscout.structures.Vector;

public class SequentialOutlierDetector extends OutlierDetector {

    private static final long serialVersionUID = 1L;

    public SequentialOutlierDetector(int dim, double eps, int minPts) {
        this.dim = dim;
        this.eps = eps;
        this.minPts = minPts;
    }

    @Override
    public void run(String inputPath, String outputPath, boolean printStats) {
        /* Create the grid */
        Map<Cell, List<Vector>> allPoints = parseInputAndCreateGrid(inputPath);

        /* Get core points */
        Map<Cell, List<Vector>> corePoints = findCorePoints(allPoints);

        /* Get outliers */
        Map<Cell, List<Vector>> outliers = findOutliers(allPoints, corePoints);

        /* Save output file */
        saveResults(outliers, outputPath);

        /* Print statistics */
        if (printStats)
            System.out.print(statistics(allPoints, corePoints, outliers));
    }

    /**
     * Extracts statistics from the execution results.
     * 
     * @param allCells The PairRDD representing the input vectors.
     * @param coreCells The PairRDD representing the core points.
     * @param outliers The PairRDD representing the outliers.
     * @param cellMap The constructed cell map.
     * @return A statistics string.
     */
    private String statistics(Map<Cell, List<Vector>> allPoints, Map<Cell, List<Vector>> corePoints, Map<Cell, List<Vector>> outliers) {
        long countPoints = 0, countCorePoints = 0, countBorderPoints = 0, countOutliers = 0;
        long countCells = 0, countDenseCells = 0, countCoreCells = 0, countOtherCells = 0;
        long maxPointsPerCell = 0, minPointsPerCell = Integer.MAX_VALUE;
        long maxNeighborsPerCell = 0, minNeighborsPerCell = Integer.MAX_VALUE, totalNeighborsPerCell = 0;

        for (Entry<Cell, List<Vector>> e : allPoints.entrySet()) {
            /* Count total points and cells */
            countPoints += e.getValue().size();
            countCells++;

            /* Update counts per cell type */
            if (e.getValue().size() >= minPts)
                countDenseCells++;
            else if (corePoints.containsKey(e.getKey()))
                countCoreCells++;
            else
                countOtherCells++;

            /* Update max and min points per cell */
            if (e.getValue().size() > maxPointsPerCell)
                maxPointsPerCell = e.getValue().size();
            if (e.getValue().size() < minPointsPerCell)
                minPointsPerCell = e.getValue().size();
            
            /* Count not empty neighbors */
            int notEmptyNeighbors = 0;
            for (Cell n : e.getKey().generateNeighbors()) {
                if (allPoints.containsKey(n)) {
                    notEmptyNeighbors++;
                    totalNeighborsPerCell++;
                }
            }

            /* Update max and min neighbors per cell */
            if (notEmptyNeighbors > maxNeighborsPerCell)
                maxNeighborsPerCell = notEmptyNeighbors;
            if (notEmptyNeighbors < minNeighborsPerCell)
                minNeighborsPerCell = notEmptyNeighbors;
        }

        /* Count core and outlier points */
        for (Entry<Cell, List<Vector>> e : corePoints.entrySet())
            countCorePoints += e.getValue().size();
        for (Entry<Cell, List<Vector>> e : outliers.entrySet())
            countOutliers += e.getValue().size();
        
        countBorderPoints = countPoints - countCorePoints - countOutliers;
        
        /* Print statistics */
        return
            "Eps: " + eps + "\n" +
            "MinPts: " + minPts + "\n" +
            "Total points: " + countPoints + "\n" +
            "Core points: " + countCorePoints + " (" + ((double) countCorePoints / countPoints * 100) + "%)" + "\n" +
            "Border points: " + countBorderPoints + " (" + ((double) countBorderPoints / countPoints * 100) + "%)" + "\n" +
            "Outliers: " + countOutliers + " (" + ((double) countOutliers / countPoints * 100) + "%)" + "\n" +
            "Total cells: " + countCells + "\n" +
            "Dense cells: " + countDenseCells + " (" + ((double) countDenseCells / countCells * 100) + "%)" + "\n" +
            "Core cells: " + countCoreCells + " (" + ((double) countCoreCells / countCells * 100) + "%)" + "\n" +
            "Other cells: " + countOtherCells + " (" + ((double) countOtherCells / countCells * 100) + "%)" + "\n" +
            "Max points per cell: " + maxPointsPerCell + "\n" +
            "Min points per cell: " + minPointsPerCell + "\n" +
            "Avg points per cell: " + (double) countPoints / allPoints.keySet().size() + "\n" +
            "Max neighbors per cell: " + maxNeighborsPerCell + "\n" +
            "Min neighbors per cell: " + minNeighborsPerCell + "\n" +
            "Avg neighbors per cell: " + (double) totalNeighborsPerCell / allPoints.keySet().size() + "\n";
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
            long lineCount = 0;

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
                    pos[i] = (int) Math.floor(coords[i] / eps * Math.sqrt(dim));
                }

                /* Construct the cell and vector */
                Cell c = new Cell(pos);
                Vector v = new Vector(lineCount, coords);

                /* Insert the point in the map */
                if (!allPoints.containsKey(c))
                    allPoints.put(c, new ArrayList<>());
                allPoints.get(c).add(v);

                /* Increment line count */
                lineCount++;
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

                                    if (countNeighbors >= minPts) break;
                                }
                            }

                            if (countNeighbors >= minPts) break;
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
                                    break;
                                }
                            }

                            if (!isOutlier) break;
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
                    bw.write(v.toString().substring(1, v.toString().length() - 1) + "\n");
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}