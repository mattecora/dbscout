package it.polito.s256654.thesis;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.spark.api.java.JavaDoubleRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.ml.linalg.Vector;
import org.apache.spark.ml.linalg.Vectors;
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
     * Computes the distance between two vectors.
     * 
     * @param v1 The first vector.
     * @param v2 The second vector.
     * @return The distance between the two vectors.
     */
    private double distance(Vector v1, Vector v2) {
        double sum = 0;

        for (int i = 0; i < dim; i++)
            sum += Math.pow(v1.apply(i) - v2.apply(i), 2);

        return Math.sqrt(sum);
    }

    /**
     * Computes the minimum distance between two cells.
     * 
     * @param c1 The first cell.
     * @param c2 The second cell.
     * @return The minimum distance between the two cells.
     */
    private double minCellDistance(Cell c1, Cell c2) {
        double sum = 0;

        for (int i = 0; i < dim; i++) {
            int axisDistance = Math.abs(c1.getPos()[i] - c2.getPos()[i]) - 1;
            sum += axisDistance <= 0 ? 0 : Math.pow(axisDistance, 2);
        }

        return eps * Math.sqrt(sum / dim);
    }

    /**
     * Generates the neighbors of a given cell.
     * 
     * @param cell The cell whose neighbors have to be generated.
     * @return The list of neighbors.
     */
    private List<Cell> generateNeighbors(Cell cell) {
        int delta = (int) Math.ceil(Math.sqrt(dim));
        List<Cell> neighbors = new ArrayList<>();

        generateNeighborsRec(cell, 0, delta, new int[dim], neighbors);
        return neighbors;
    }

    /**
     * Recursive function to generate the neighbors.
     * 
     * @param cell The cell whose neighbors have to be generated.
     * @param x The position to be considered.
     * @param delta The size of the surrounding frame to be considered.
     * @param newPos The newly generated position.
     * @param neighbors The list of neighbors to be populated.
     */
    private void generateNeighborsRec(Cell cell, int x, int delta, int[] newPos, List<Cell> neighbors) {
        if (x == dim) {
            /* Create the new cell */
            Cell newCell = new Cell(Arrays.copyOf(newPos, newPos.length));

            /* Add the cell to the neighbors if its minimum distance is at most eps */
            if (minCellDistance(cell, newCell) < eps)
                neighbors.add(new Cell(Arrays.copyOf(newPos, newPos.length)));
            return;
        }

        /* Generate a dimension and go to the next */
        for (int i = cell.getPos()[x] - delta; i <= cell.getPos()[x] + delta; i++) {
            newPos[x] = i;
            generateNeighborsRec(cell, x + 1, delta, newPos, neighbors);
        }
    }

    /**
     * Locally generate a cell map, which is then distributed to all executors.
     * 
     * @param rdd1 The RDD containing the cells of the first type.
     * @param rdd2 The RDD containing the cells of the second type.
     * @param type1 The type of the cells of the first RDD.
     * @param type2 The type of the cells of the second RDD.
     * @return The broadcasted cell map.
     */
    private Broadcast<CellMap> generateCellMap(JavaPairRDD<Cell, Vector> rdd1, JavaPairRDD<Cell, Vector> rdd2, CellType type1, CellType type2) {
        /* Create a new cell map */
        CellMap cellMapLocal = new CellMap();

        /* Collect the keys and construct the cell map */
        rdd1.keys().distinct().collect().forEach(c -> cellMapLocal.putCell(c, type1));
        rdd2.keys().distinct().collect().forEach(c -> cellMapLocal.putCell(c, type2));

        /* Broadcast the cell map */
        return JavaSparkContext.fromSparkContext(rdd1.context()).broadcast(cellMapLocal);
    }

    /**
     * Runs the algorithm, coordinating the execution of the different tasks.
     * 
     * @param dataset The RDD containing all the points.
     * @return An RDD containing all the outliers.
     */
    public JavaRDD<Vector> run(String inputPath) {
        /* Create the grid */
        JavaPairRDD<Cell, Vector> allCells = parseInputAndCreateGrid(inputPath).persist(StorageLevel.MEMORY_AND_DISK_SER());

        /* Get dense cells */
        JavaPairRDD<Cell, Vector> denseCells = getDenseCells(allCells).persist(StorageLevel.MEMORY_AND_DISK_SER());

        /* Get non-dense cells */
        JavaPairRDD<Cell, Vector> nonDenseCells = allCells.subtractByKey(denseCells);

        /* Create the dense/non-dense cell map */
        Broadcast<CellMap> denseCellMap = generateCellMap(denseCells, nonDenseCells, CellType.DENSE, CellType.NON_DENSE);

        /* Get core points */
        JavaPairRDD<Cell, Vector> coreCells = findCoreCells(allCells, denseCells, nonDenseCells, denseCellMap).persist(StorageLevel.MEMORY_AND_DISK_SER());

        /* Get non-core points */
        JavaPairRDD<Cell, Vector> nonCoreCells = nonDenseCells.subtractByKey(coreCells);

        /* Create the core/non-core cell map */
        Broadcast<CellMap> coreCellMap = generateCellMap(coreCells, nonCoreCells, CellType.CORE, CellType.NON_CORE);

        /* Get outliers */
        return findOutliers(coreCells, nonCoreCells, coreCellMap);
    }

    public void statistics(String inputPath) {
        /* Create the grid */
        JavaPairRDD<Cell, Vector> allCells = parseInputAndCreateGrid(inputPath).persist(StorageLevel.MEMORY_AND_DISK_SER());

        /* Get dense cells */
        JavaPairRDD<Cell, Vector> denseCells = getDenseCells(allCells).persist(StorageLevel.MEMORY_AND_DISK_SER());

        /* Get non-dense cells */
        JavaPairRDD<Cell, Vector> nonDenseCells = allCells.subtractByKey(denseCells);

        /* Create the dense/non-dense cell map */
        Broadcast<CellMap> denseCellMap = generateCellMap(denseCells, nonDenseCells, CellType.DENSE, CellType.NON_DENSE);

        /* Count points per cell */
        JavaDoubleRDD pointsPerCell = allCells
            .mapValues(v -> 1)
            .reduceByKey((v1, v2) -> v1 + v2)
            .mapToDouble(p -> p._2())
            .cache();

        /* Count neighbors per cell */
        JavaDoubleRDD neighborsPerCell = allCells
            .distinct()
            .mapToDouble(p -> {
                int numNeighbors = 0;

                for (Cell n : generateNeighbors(p._1())) {
                    if (denseCellMap.value().getCellType(n) != CellType.EMPTY)
                        numNeighbors++;
                }

                return numNeighbors;
            })
            .cache();

        /* Print statistics */
        System.out.print(
            "Eps: " + eps + "\n" +
            "MinPts: " + minPts + "\n" +
            "Total cells: " + allCells.keys().distinct().count() + "\n" +
            "Dense cells: " + denseCells.keys().distinct().count() + "\n" +
            "Max points per cell: " + pointsPerCell.max() + "\n" +
            "Min points per cell: " + pointsPerCell.min() + "\n" +
            "Avg points per cell: " + pointsPerCell.sum() / pointsPerCell.count() + "\n" +
            "Max neighbors per cell: " + neighborsPerCell.max() + "\n" +
            "Min neighbors per cell: " + neighborsPerCell.min() + "\n" +
            "Avg neighbors per cell: " + neighborsPerCell.sum() / neighborsPerCell.count() + "\n"
        );
    }

    /**
     * Creates the 2D grid of points with diagonal eps.
     * 
     * @param dataset The RDD containing all points.
     * @return A PairRDD containing, for all cell, the corresponding points.
     */
    private JavaPairRDD<Cell, Vector> parseInputAndCreateGrid(String inputPath) {
        JavaPairRDD<Cell, Vector> allCells = sc.textFile(inputPath)
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
                return new Tuple2<>(new Cell(pos), Vectors.dense(coords));
            });

        return allCells;
    }

    /**
     * Returns the dense cells (containing at least minPts points).
     * 
     * @param allCells The PairRDD representing the 2D grid.
     * @return A PairRDD containing only the dense cells.
     */
    private JavaPairRDD<Cell, Vector> getDenseCells(JavaPairRDD<Cell, Vector> allCells) {
        JavaPairRDD<Cell, Vector> denseCells = allCells
            .mapValues(v -> 1)                      /* Emit pairs (cell, 1) */
            .reduceByKey((v1, v2) -> v1 + v2)       /* Count points per cell */
            .filter(p -> p._2() >= minPts)          /* Filter dense cells */
            .join(allCells)                         /* Join with the original dataset */
            .mapValues(v -> v._2());                /* Drop the count value */
        
        return denseCells;
    }

    /**
     * Returns the core points contained in each cell.
     * 
     * @param allCells The PairRDD representing the 2D grid.
     * @param denseCells The PairRDD containing only the dense cells.
     * @param nonDenseCells The PairRDD containing only the non-dense cells.
     * @return A PairRDD containing the core points for each cell.
     */
    private JavaPairRDD<Cell, Vector> findCoreCells(JavaPairRDD<Cell, Vector> allCells, JavaPairRDD<Cell, Vector> denseCells, JavaPairRDD<Cell, Vector> nonDenseCells, Broadcast<CellMap> denseCellMap) {
        /* List points to check for every cell */
        JavaPairRDD<Cell, Vector> pointsToCheck = allCells
            .flatMapToPair(p -> {
                List<Cell> neighbors = generateNeighbors(p._1());
                List<Tuple2<Cell, Vector>> tuples = new ArrayList<>();

                /* Emit a pair (neighboring cell, point to be checked) */
                for (Cell n : neighbors) {
                    if (denseCellMap.value().getCellType(n) == CellType.NON_DENSE)
                        tuples.add(new Tuple2<>(n, p._2()));
                }

                return tuples.iterator();
            });
        
        /* Get core points from non-dense cells */
        JavaPairRDD<Cell, Vector> partiallyCoreCells = nonDenseCells
            .join(pointsToCheck)                                        /* Join with the points to be checked */
            .mapToPair(p -> {
                /* Check distance between points */
                double d = distance(p._2()._1(), p._2()._2());

                /* Emit a pair ((cell, point), distance < eps) */
                return new Tuple2<>(new Tuple2<>(p._1(), p._2()._1()), d < eps ? 1 : 0);
            })
            .reduceByKey((v1, v2) -> v1 + v2)                           /* Count points with distance < eps */
            .filter(p -> p._2() >= minPts)                              /* Filter core points */
            .mapToPair(p -> new Tuple2<>(p._1()._1(), p._1()._2()));    /* Emit a pair (cell, point) */

        return denseCells.union(partiallyCoreCells);
    }

    /**
     * Returns the outliers.
     * 
     * @param coreCells The PairRDD containing only the core cells.
     * @param nonCoreCells The PairRDD containing only the non-core cells.
     * @return An RDD containing all the outliers.
     */
    private JavaRDD<Vector> findOutliers(JavaPairRDD<Cell, Vector> coreCells, JavaPairRDD<Cell, Vector> nonCoreCells, Broadcast<CellMap> coreCellMap) {
        /* List points to check for every cell */
        JavaPairRDD<Cell, Vector> pointsToCheck = coreCells
            .flatMapToPair(p -> {
                List<Cell> neighbors = generateNeighbors(p._1());
                List<Tuple2<Cell, Vector>> tuples = new ArrayList<>();

                /* Emit a pair (neighboring cell, point to be checked) */
                for (Cell n : neighbors) {
                    if (coreCellMap.value().getCellType(n) == CellType.NON_CORE)
                        tuples.add(new Tuple2<>(n, p._2()));
                }

                return tuples.iterator();
            });
        
        /* Get the list of outliers */
        JavaRDD<Vector> outliers = nonCoreCells
            .leftOuterJoin(pointsToCheck)                   /* Join with the points to be checked */
            .mapToPair(p -> {
                /* A point is an outlier if it has no neighbor or distance >= eps */
                boolean o = !p._2()._2().isPresent() || distance(p._2()._1(), p._2()._2().get()) >= eps;

                /* Emit a pair ((cell, point), outlier or not) */
                return new Tuple2<>(new Tuple2<>(p._1(), p._2()._1()), o);
            })
            .reduceByKey((v1, v2) -> v1 && v2)              /* Combine information from all points */
            .filter(p -> p._2())                            /* Filter outliers */
            .map(p -> p._1()._2());                         /* Map to the original vectors */
        
        return outliers;
    }

}