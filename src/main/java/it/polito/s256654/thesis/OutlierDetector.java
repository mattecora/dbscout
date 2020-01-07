package it.polito.s256654.thesis;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.ml.linalg.Vector;

import scala.Tuple2;

public class OutlierDetector implements Serializable {
    
    private static final long serialVersionUID = 1L;
    
    private double eps;
    private int minPts;

    public OutlierDetector(double eps, int minPts) {
        this.eps = eps;
        this.minPts = minPts;
    }

    /**
     * Compute the distance between two vectors.
     * 
     * @param v1 The first vector.
     * @param v2 The second vector.
     * @return The distance between the two vectors.
     */
    private double distance(Vector v1, Vector v2) {
        return Math.sqrt(Math.pow(v1.apply(0) - v2.apply(0), 2) + Math.pow(v1.apply(1) - v2.apply(1), 2));
    }

    /**
     * Runs the algorithm, coordinating the execution of the different tasks.
     * 
     * @param dataset The RDD containing all the points.
     * @return An RDD containing all the outliers.
     */
    public JavaRDD<Vector> run(JavaRDD<Vector> dataset) {
        /* Create the grid */
        JavaPairRDD<Cell, Vector> allCells = createGrid(dataset);

        /* Get dense cells */
        JavaPairRDD<Cell, Vector> denseCells = getDenseCells(allCells);

        /* Get non-dense cells */
        JavaPairRDD<Cell, Vector> nonDenseCells = allCells.subtractByKey(denseCells);

        /* Get core points */
        JavaPairRDD<Cell, Vector> coreCells = findCoreCells(allCells, denseCells, nonDenseCells);

        /* Get non-core points */
        JavaPairRDD<Cell, Vector> nonCoreCells = nonDenseCells.subtractByKey(coreCells);

        /* Get outliers */
        return findOutliers(coreCells, nonCoreCells);
    }

    /**
     * Creates the 2D grid of points with diagonal eps.
     * 
     * @param dataset The RDD containing all points.
     * @return A PairRDD containing, for all cell, the corresponding points.
     */
    private JavaPairRDD<Cell, Vector> createGrid(JavaRDD<Vector> dataset) {
        JavaPairRDD<Cell, Vector> allCells = dataset
            .mapToPair(p -> {
                /* Compute cell coordinates */
                int cellX = (int) (p.apply(0) / eps);
                int cellY = (int) (p.apply(1) / eps);

                /* Emit a pair (cell, point) */
                return new Tuple2<>(new Cell(cellX, cellY), p);
            })
            .cache();

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
            .mapValues(v -> v._2())                 /* Drop the count value */
            .cache();
        
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
    private JavaPairRDD<Cell, Vector> findCoreCells(JavaPairRDD<Cell, Vector> allCells, JavaPairRDD<Cell, Vector> denseCells, JavaPairRDD<Cell, Vector> nonDenseCells) {
        /* List points to check for every cell */
        JavaPairRDD<Cell, Vector> pointsToCheck = allCells
            .flatMapToPair(p -> {
                List<Tuple2<Cell, Vector>> tuples = new ArrayList<>();

                for (int i = p._1().getxPos() - 2; i <= p._1().getxPos() + 2; i++) {
                    for (int j = p._1().getyPos() - 2; j <= p._1().getyPos() + 2; j++) {
                        /* Do not consider the diagonal cells */
                        if ((i == p._1().getxPos() - 2 && j == p._1().getyPos() - 2) || 
                            (i == p._1().getxPos() - 2 && j == p._1().getyPos() + 2) || 
                            (i == p._1().getxPos() + 2 && j == p._1().getyPos() - 2) ||
                            (i == p._1().getxPos() + 2 && j == p._1().getyPos() + 2))
                            continue;
                        
                        /* Emit a pair (neighboring cell, point to be checked) */
                        tuples.add(new Tuple2<>(new Cell(i, j), p._2()));
                    }
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
    private JavaRDD<Vector> findOutliers(JavaPairRDD<Cell, Vector> coreCells, JavaPairRDD<Cell, Vector> nonCoreCells) {
        /* List points to check for every cell */
        JavaPairRDD<Cell, Vector> pointsToCheck = coreCells
            .flatMapToPair(p -> {
                List<Tuple2<Cell, Vector>> tuples = new ArrayList<>();

                for (int i = p._1().getxPos() - 2; i <= p._1().getxPos() + 2; i++) {
                    for (int j = p._1().getyPos() - 2; j <= p._1().getyPos() + 2; j++) {
                        /* Do not consider the diagonal cells */
                        if ((i == p._1().getxPos() - 2 && j == p._1().getyPos() - 2) || 
                            (i == p._1().getxPos() - 2 && j == p._1().getyPos() + 2) || 
                            (i == p._1().getxPos() + 2 && j == p._1().getyPos() - 2) ||
                            (i == p._1().getxPos() + 2 && j == p._1().getyPos() + 2))
                            continue;
                        
                        /* Emit a pair (neighboring cell, point to be checked) */
                        tuples.add(new Tuple2<>(new Cell(i, j), p._2()));
                    }
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