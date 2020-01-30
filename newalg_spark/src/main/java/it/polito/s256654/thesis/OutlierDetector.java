package it.polito.s256654.thesis;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.spark.api.java.JavaDoubleRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.storage.StorageLevel;

import it.polito.s256654.thesis.CellMap.CellType;
import scala.Tuple2;

public class OutlierDetector implements Serializable {
    
    private static final long serialVersionUID = 1L;
    
    private transient JavaSparkContext sc;
    private int dim;
    private double eps;
    private int minPts;

    public OutlierDetector(JavaSparkContext sc, int dim, double eps, int minPts) {
        this.sc = sc;
        this.dim = dim;
        this.eps = eps;
        this.minPts = minPts;
    }

    /**
     * Runs the algorithm, coordinating the execution of the different tasks.
     * 
     * @param dataset The RDD containing all the points.
     * @return An RDD containing all the outliers.
     */
    public void run(String inputPath, String outputPath, boolean stats, boolean broadcastJoin) {
        /* Create the grid */
        JavaPairRDD<Cell, Vector> allCells = parseInputAndCreateGrid(inputPath).persist(StorageLevel.MEMORY_AND_DISK_SER());

        /* Create the dense/non-dense cell map */
        Broadcast<CellMap> denseCellMap = buildDenseCellMap(allCells);

        /* Get core points */
        JavaPairRDD<Cell, Vector> coreCells = findCoreCells(allCells, denseCellMap, broadcastJoin).persist(StorageLevel.MEMORY_AND_DISK_SER());

        /* Get non-core points */
        JavaPairRDD<Cell, Vector> nonCoreCells = allCells.subtractByKey(coreCells);

        /* Get outliers */
        JavaPairRDD<Cell, Vector> outliers = findOutliers(coreCells, nonCoreCells, denseCellMap);

        /* Save output file */
        outliers
            .map(p -> p._2().toString().substring(1, p._2().toString().length() - 1))
            .saveAsTextFile(outputPath);
        
        /* Print statistics */
        if (stats)
            System.out.print(statistics(allCells, denseCellMap));
    }

    private String statistics(JavaPairRDD<Cell, Vector> allCells, Broadcast<CellMap> denseCellMap) {
        /* Count points per cell */
        JavaDoubleRDD pointsPerCell = allCells
            .mapValues(v -> 1)
            .reduceByKey((v1, v2) -> v1 + v2)
            .mapToDouble(p -> p._2())
            .cache();

        /* Count neighbors per cell */
        JavaDoubleRDD neighborsPerCell = allCells
            .keys()
            .distinct()
            .mapToDouble(c -> {
                int numNeighbors = 0;

                for (Cell n : c.generateNeighbors()) {
                    if (denseCellMap.value().getCellType(n) != CellType.EMPTY)
                        numNeighbors++;
                }

                return numNeighbors;
            })
            .cache();
        
        /* Print statistics */
        return
            "Eps: " + eps + "\n" +
            "MinPts: " + minPts + "\n" +
            "Total cells: " + denseCellMap.value().getTotalCellsNum() + "\n" +
            "Dense cells: " + denseCellMap.value().getDenseCellsNum() + "\n" +
            "Max points per cell: " + pointsPerCell.max() + "\n" +
            "Min points per cell: " + pointsPerCell.min() + "\n" +
            "Avg points per cell: " + pointsPerCell.sum() / pointsPerCell.count() + "\n" +
            "Max neighbors per cell: " + neighborsPerCell.max() + "\n" +
            "Min neighbors per cell: " + neighborsPerCell.min() + "\n" +
            "Avg neighbors per cell: " + neighborsPerCell.sum() / neighborsPerCell.count() + "\n";
    }

    /**
     * Parses the input vectors and constructs the grid of points with diagonal eps.
     * 
     * @param inputPath The path of the files to be parsed.
     * @return A PairRDD containing, for all cells, the corresponding points.
     */
    private JavaPairRDD<Cell, Vector> parseInputAndCreateGrid(String inputPath) {
        JavaPairRDD<Cell, Vector> allCells = sc.textFile(inputPath)
            .filter(s -> !s.startsWith("x"))
            .mapToPair(s -> {
                String[] tokens = s.split(",");
                double[] coords = new double[dim];
                int[] pos = new int[dim];

                /* Compute cell coordinates */
                for (int i = 0; i < dim; i++) {
                    coords[i] = Double.parseDouble(tokens[i]);
                    pos[i] = (int) (coords[i] / eps * Math.sqrt(dim));
                }

                /* Emit a pair (cell, point) */
                return new Tuple2<>(new Cell(pos), new Vector(coords));
            });

        return allCells;
    }

    /**
     * Constructs and broadcasts the cell map.
     * 
     * @param allCells The PairRDD representing the input vectors.
     * @return The broadcast cell map.
     */
    private Broadcast<CellMap> buildDenseCellMap(JavaPairRDD<Cell, Vector> allCells) {
        JavaPairRDD<Cell, Integer> cellsCount = allCells
            .mapValues(v -> 1)                              /* Emit pairs (cell, 1) */
            .reduceByKey((v1, v2) -> v1 + v2);              /* Count points per cell */
        
        /* Create a new cell map */
        CellMap cellMapLocal = new CellMap();

        /* Collect the keys and construct the cell map */
        for (Tuple2<Cell, Integer> p : cellsCount.collect()) {
            if (p._2() >= minPts)
                cellMapLocal.putCell(p._1(), CellType.DENSE);
            else
                cellMapLocal.putCell(p._1(), CellType.NON_DENSE);
        }

        /* Broadcast the cell map */
        return sc.broadcast(cellMapLocal);
    }

    /**
     * Returns the core points contained in each cell.
     * 
     * @param allCells The PairRDD representing the input vectors.
     * @param denseCellMap The constructed cell map.
     * @return A PairRDD containing the core points for each cell.
     */
    private JavaPairRDD<Cell, Vector> findCoreCells(JavaPairRDD<Cell, Vector> allCells, Broadcast<CellMap> denseCellMap, boolean broadcastJoin) {
        /* List points to check for every cell */
        JavaPairRDD<Cell, Tuple2<Cell, Vector>> pointsToCheck = allCells
            .filter(p -> denseCellMap.value().getCellType(p._1()) == CellType.NON_DENSE)
            .flatMapToPair(p -> {
                List<Cell> neighbors = p._1().generateNeighbors();
                List<Tuple2<Cell, Tuple2<Cell, Vector>>> tuples = new ArrayList<>();

                /* Emit a pair (neighboring cell, point to be checked) */
                for (Cell n : neighbors) {
                    if (denseCellMap.value().getCellType(n) != CellType.EMPTY)
                        tuples.add(new Tuple2<>(n, p));
                }

                return tuples.iterator();
            });
        
        JavaPairRDD<Tuple2<Cell, Vector>, Integer> joinedPoints;

        if (broadcastJoin) {
            /* Collect and broadcast pointsToCheck */
            Map<Cell, Iterable<Tuple2<Cell, Vector>>> pointsToCheckLocal = new HashMap<>();
            pointsToCheckLocal.putAll(pointsToCheck.groupByKey().collectAsMap());

            Broadcast<Map<Cell, Iterable<Tuple2<Cell, Vector>>>> pointsToCheckBc = sc.broadcast(pointsToCheckLocal);

            /* Perform a broadcast join */
            joinedPoints = allCells
                .flatMapToPair(p -> {
                    List<Tuple2<Tuple2<Cell, Vector>, Integer>> joinedTuples = new ArrayList<>();

                    if (pointsToCheckBc.value().containsKey(p._1())) {
                        for (Tuple2<Cell, Vector> p2 : pointsToCheckBc.value().get(p._1())) {
                            /* Check distance between points */
                            double d = p._2().distanceTo(p2._2());

                            /* Emit a pair ((cell, point), distance < eps) */
                            joinedTuples.add(new Tuple2<>(p2, d < eps ? 1 : 0));
                        }
                    }

                    return joinedTuples.iterator();
                });
        } else {
            /* Get core points from non-dense cells */
            joinedPoints = allCells
                .join(pointsToCheck)                            /* Join with the points to be checked */
                .mapToPair(p -> {
                    /* Check distance between points */
                    double d = p._2()._1().distanceTo(p._2()._2()._2());

                    /* Emit a pair ((cell, point), distance < eps) */
                    return new Tuple2<>(p._2()._2(), d < eps ? 1 : 0);
                });
        }

        JavaPairRDD<Cell, Vector> partiallyCoreCells = joinedPoints
            .reduceByKey((v1, v2) -> v1 + v2)               /* Count points with distance < eps */
            .filter(p -> p._2() >= minPts)                  /* Filter core points */
            .mapToPair(p -> p._1());                        /* Emit a pair (cell, point) */

        return allCells
            .filter(p -> denseCellMap.value().getCellType(p._1()) == CellType.DENSE)
            .union(partiallyCoreCells);
    }

    /**
     * Returns the outliers for each cell.
     * 
     * @param coreCells The PairRDD containing only the core cells.
     * @param nonCoreCells The PairRDD containing only the non-core cells.
     * @param denseCellMap The constructed cell map.
     * @return A PairRDD containing the outliers for each cell.
     */
    private JavaPairRDD<Cell, Vector> findOutliers(JavaPairRDD<Cell, Vector> coreCells, JavaPairRDD<Cell, Vector> nonCoreCells, Broadcast<CellMap> denseCellMap) {
        /* List points to check for every cell */
        JavaPairRDD<Cell, Tuple2<Cell, Vector>> pointsToCheck = nonCoreCells
            .flatMapToPair(p -> {
                List<Cell> neighbors = p._1().generateNeighbors();
                List<Tuple2<Cell, Tuple2<Cell, Vector>>> tuples = new ArrayList<>();

                /* Emit a pair (neighboring cell, point to be checked) */
                for (Cell n : neighbors) {
                    if (denseCellMap.value().getCellType(n) != CellType.EMPTY)
                        tuples.add(new Tuple2<>(n, p));
                }

                return tuples.iterator();
            });
        
        /* Get the list of outliers */
        JavaPairRDD<Cell, Vector> outliers = coreCells
            .rightOuterJoin(pointsToCheck)                   /* Join with the points to be checked */
            .mapToPair(p -> {
                /* A point is an outlier if it has no neighbor or distance >= eps */
                boolean o = !p._2()._1().isPresent() || p._2()._1().get().distanceTo(p._2()._2()._2()) >= eps;

                /* Emit a pair ((cell, point), outlier or not) */
                return new Tuple2<>(p._2()._2(), o);
            })
            .reduceByKey((v1, v2) -> v1 && v2)              /* Combine information from all points */
            .filter(p -> p._2())                            /* Filter outliers */
            .mapToPair(p -> p._1());                        /* Map to the original vectors */
        
        return outliers;
    }

}