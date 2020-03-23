package it.polito.s256654.thesis.algorithm.parallel;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaDoubleRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.storage.StorageLevel;

import it.polito.s256654.thesis.algorithm.OutlierDetector;
import it.polito.s256654.thesis.structures.Cell;
import it.polito.s256654.thesis.structures.CellMap;
import it.polito.s256654.thesis.structures.Vector;
import it.polito.s256654.thesis.structures.CellMap.CellType;
import scala.Tuple2;

public class ParallelOutlierDetector extends OutlierDetector {

    private static final long serialVersionUID = 1L;
    
    protected transient JavaSparkContext sc;

    public ParallelOutlierDetector(int dim, double eps, int minPts) {
        this.dim = dim;
        this.eps = eps;
        this.minPts = minPts;
    }

    @Override
    public void run(String inputPath, String outputPath, boolean printStats) {
        run(inputPath, outputPath, printStats, 0);
    }

    /**
     * Runs the algorithm, coordinating the execution of the different tasks.
     * 
     * @param inputPath The path of the input files.
     * @param outputPath The path for the output files.
     * @param stats Print statistics to the standard output.
     * @param numPart The desired number of partitions (0 for default).
     */
    public void run(String inputPath, String outputPath, boolean printStats, int numPart) {
        /* Define the Spark context */
        SparkConf conf = new SparkConf()
            .setAppName("Outlier detector")
            .registerKryoClasses(new Class<?>[]{this.getClass(), Cell.class, CellMap.class, Vector.class});
        sc = new JavaSparkContext(conf);

        /* Create the grid */
        JavaPairRDD<Cell, Vector> allCells = parseInputAndCreateGrid(inputPath, numPart)
            .persist(StorageLevel.MEMORY_AND_DISK());

        /* Create the dense/non-dense cell map */
        Broadcast<CellMap> denseCellMap = buildDenseCellMap(allCells);

        /* Get points from non-dense cells */
        JavaPairRDD<Cell, Vector> nonDenseCells = allCells
            .filter(p -> denseCellMap.value().getCellType(p._1()) == CellType.OTHER)
            .persist(StorageLevel.MEMORY_AND_DISK());

        /* Get core points from non-dense cells */
        JavaPairRDD<Cell, Vector> partiallyCoreCells = findPartiallyCoreCells(allCells, nonDenseCells, denseCellMap)
            .persist(StorageLevel.MEMORY_AND_DISK());

        /* Update the cell map with core cells */
        Broadcast<CellMap> coreCellMap = buildCoreCellMap(partiallyCoreCells, denseCellMap);

        /* Get the entire set of core points */
        JavaPairRDD<Cell, Vector> coreCells = allCells
            .filter(p -> denseCellMap.value().getCellType(p._1()) == CellType.DENSE)
            .union(partiallyCoreCells);

        /* Select cells with no core point */
        JavaPairRDD<Cell, Vector> nonCoreCells = nonDenseCells
            .filter(p -> coreCellMap.value().getCellType(p._1()) == CellType.OTHER)
            .persist(StorageLevel.MEMORY_AND_DISK());

        /* Get outliers */
        JavaPairRDD<Cell, Vector> outliers = findOutliers(coreCells, nonCoreCells, coreCellMap);

        /* Save output file */
        outliers.map(p -> p._2().toCSVRow()).saveAsTextFile(outputPath);
        
        /* Close the Spark context */
        sc.close();
        
        /* Print statistics */
        if (printStats)
            System.out.print(statistics(allCells, coreCells, outliers, coreCellMap));
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
    private String statistics(JavaPairRDD<Cell, Vector> allCells, JavaPairRDD<Cell, Vector> coreCells, JavaPairRDD<Cell, Vector> outliers, Broadcast<CellMap> cellMap) {
        /* Get cell statistics */
        Map<CellType, Long> cellCounts = cellMap.value().getCellsCount();

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
            .mapToDouble(c -> cellMap.value().getNotEmptyNeighborsOf(c).size())
            .cache();
        
        /* Print statistics */
        return
            "Eps: " + eps + "\n" +
            "MinPts: " + minPts + "\n" +
            "Total points: " + allCells.count() + "\n" +
            "Core points: " + coreCells.count() + "\n" +
            "Outliers: " + outliers.count() + "\n" +
            "Total cells: " + cellMap.value().getTotalCellsNum() + "\n" +
            "Dense cells: " + cellCounts.get(CellType.DENSE) + "\n" +
            "Core cells: " + cellCounts.get(CellType.CORE)+ "\n" +
            "Other cells: " + cellCounts.get(CellType.OTHER) + "\n" +
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
     * @param numPart The desired number of partitions (0 for default).
     * @return A PairRDD containing, for all cells, the corresponding points.
     */
    protected JavaPairRDD<Cell, Vector> parseInputAndCreateGrid(String inputPath, int numPart) {
        JavaPairRDD<Cell, Vector> allCells = (numPart == 0 ? sc.textFile(inputPath) : sc.textFile(inputPath, numPart))
            .filter(s -> !s.startsWith("x"))
            .zipWithUniqueId()
            .mapToPair(p -> {
                String[] tokens = p._1().split(",");
                double[] coords = new double[dim];
                int[] pos = new int[dim];

                /* Compute cell coordinates */
                for (int i = 0; i < dim; i++) {
                    coords[i] = Double.parseDouble(tokens[i]);
                    pos[i] = (int) Math.floor(coords[i] / eps * Math.sqrt(dim));
                }

                /* Emit a pair (cell, point) */
                return new Tuple2<>(new Cell(pos), new Vector(p._2(), coords));
            });

        return allCells;
    }

    /**
     * Constructs and broadcasts the dense/non-dense cell map.
     * 
     * @param allCells The PairRDD representing the input vectors.
     * @return The broadcast cell map.
     */
    protected Broadcast<CellMap> buildDenseCellMap(JavaPairRDD<Cell, Vector> allCells) {
        /* Create a local cell map */
        CellMap cellMapLocal = allCells
            .mapValues(v -> 1L)                             /* Emit pairs (cell, 1) */
            .reduceByKey((v1, v2) -> v1 + v2)               /* Count points per cell */
            .aggregate(
                new CellMap(),
                (c, p) -> c.putCell(p._1(), p._2() >= minPts ? CellType.DENSE : CellType.OTHER),
                (c1, c2) -> c1.combineWith(c2)
            );                                              /* Aggregate in a cell map */

        /* Broadcast the cell map */
        return sc.broadcast(cellMapLocal);
    }

    /**
     * Returns the core points contained in each non-core cell.
     * 
     * @param allCells The PairRDD representing the input vectors.
     * @param nonDenseCells The PairRDD containing only the vectors in non-dense cells.
     * @param cellMap The previously constructed cell map.
     * @return A PairRDD containing the core points for each non-core cell.
     */
    protected JavaPairRDD<Cell, Vector> findPartiallyCoreCells(JavaPairRDD<Cell, Vector> allCells, JavaPairRDD<Cell, Vector> nonDenseCells, Broadcast<CellMap> cellMap) {
        /* List points to check for every cell */
        JavaPairRDD<Cell, Tuple2<Cell, Vector>> pointsToCheck = nonDenseCells
            .flatMapToPair(p -> {
                List<Cell> neighbors = cellMap.value().getNotEmptyNeighborsOf(p._1());
                List<Tuple2<Cell, Tuple2<Cell, Vector>>> tuples = new ArrayList<>();

                /* Emit a pair (neighboring cell, point to be checked) */
                for (Cell n : neighbors)
                    tuples.add(new Tuple2<>(n, p));

                return tuples.iterator();
            });
        
        JavaPairRDD<Cell, Vector> partiallyCoreCells = allCells
            .join(pointsToCheck)                            /* Join with the points to be checked */
            .mapToPair(p -> {
                /* Check distance between points */
                double d = p._2()._1().distanceTo(p._2()._2()._2());

                /* Emit a pair ((cell, point), number of neighbors) */
                return new Tuple2<>(p._2()._2(), d < eps ? 1 : 0);
            })
            .reduceByKey((v1, v2) -> v1 + v2)               /* Count points with distance < eps */
            .filter(p -> p._2() >= minPts)                  /* Filter core points */
            .mapToPair(p -> p._1());                        /* Emit a pair (cell, point) */

        return partiallyCoreCells;
    }

    /**
     * Updates the cell map with partially core cells.
     * 
     * @param partiallyCoreCells The PairRDD containing the core points from non-core cells.
     * @param denseCellMap The previously constructed cell map.
     * @return The broadcast updated cell map.
     */
    protected Broadcast<CellMap> buildCoreCellMap(JavaPairRDD<Cell, Vector> partiallyCoreCells, Broadcast<CellMap> denseCellMap) {
        /* Aggregate core cells in a cell map */
        CellMap coreCellMap = partiallyCoreCells.aggregate(
            new CellMap(),
            (c, p) -> c.putCell(p._1(), CellType.CORE),
            (c1, c2) -> c1.combineWith(c2)
        );

        /* Combine with the dense cell map */
        return sc.broadcast(denseCellMap.value().combineWith(coreCellMap));
    }

    /**
     * Returns the outliers for each cell.
     * 
     * @param coreCells The PairRDD containing all the core points.
     * @param nonCoreCells The PairRDD containing the points from non-core cells.
     * @param coreCellMap The updated cell map.
     * @return A PairRDD containing the outliers for each cell.
     */
    protected JavaPairRDD<Cell, Vector> findOutliers(JavaPairRDD<Cell, Vector> coreCells, JavaPairRDD<Cell, Vector> nonCoreCells, Broadcast<CellMap> coreCellMap) {
        /* Get outliers with no neighbors */
        JavaPairRDD<Cell, Vector> outliersWithNoCoreNeighbors = nonCoreCells
            .filter(p -> coreCellMap.value().getCoreNeighborsOf(p._1()).size() == 0);
        
        /* List points to check for every cell */
        JavaPairRDD<Cell, Tuple2<Cell, Vector>> pointsToCheck = nonCoreCells
            .flatMapToPair(p -> {
                List<Cell> neighbors = coreCellMap.value().getCoreNeighborsOf(p._1());
                List<Tuple2<Cell, Tuple2<Cell, Vector>>> tuples = new ArrayList<>();

                /* Emit a pair (neighboring cell, point to be checked) */
                for (Cell n : neighbors)
                    tuples.add(new Tuple2<>(n, p));

                return tuples.iterator();
            });

        /* Get outliers from cells with neighbors */
        JavaPairRDD<Cell, Vector> outliersWithCoreNeighbors = coreCells
            .join(pointsToCheck)                    /* Join with the points to be checked */
            .mapToPair(p -> {
                /* Check distance between points */
                double d = p._2()._1().distanceTo(p._2()._2()._2());

                /* Emit a pair ((cell, point), outlier or not) */
                return new Tuple2<>(p._2()._2(), d >= eps);
            })
            .reduceByKey((v1, v2) -> v1 && v2)              /* Combine information from all points */
            .filter(p -> p._2())                            /* Filter outliers */
            .mapToPair(p -> p._1());                        /* Map to the original vectors */

        return outliersWithCoreNeighbors.union(outliersWithNoCoreNeighbors);
    }

}