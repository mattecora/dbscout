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
        JavaPairRDD<Cell, Iterable<Vector>> allCells = createGrid(dataset);

        /* Get dense cells */
        JavaPairRDD<Cell, Iterable<Vector>> denseCells = getDenseCells(allCells);

        /* Get non-dense cells */
        JavaPairRDD<Cell, Iterable<Vector>> nonDenseCells = allCells.subtractByKey(denseCells);

        /* Get core points */
        JavaPairRDD<Cell, Iterable<Vector>> coreCells = findCoreCells(allCells, denseCells, nonDenseCells);

        /* Get non-core points */
        JavaPairRDD<Cell, Iterable<Vector>> nonCoreCells = nonDenseCells.subtractByKey(coreCells);

        /* Get outliers */
        return findOutliers(coreCells, nonCoreCells);
    }

    /**
     * Creates the 2D grid of points with diagonal eps.
     * 
     * @param dataset The RDD containing all points.
     * @return A PairRDD containing, for all cell, the list of points.
     */
    private JavaPairRDD<Cell, Iterable<Vector>> createGrid(JavaRDD<Vector> dataset) {
        JavaPairRDD<Cell, Iterable<Vector>> allCells = dataset
            .mapToPair(p -> {
                int cellX = (int) (p.apply(0) / eps);
                int cellY = (int) (p.apply(1) / eps);
                return new Tuple2<>(new Cell(cellX, cellY), p);
            })
            .groupByKey()
            .cache();

        return allCells;
    }

    /**
     * Returns the dense cells (containing at least minPts points).
     * 
     * @param allCells The PairRDD representing the 2D grid.
     * @return A PairRDD containing only the dense cells.
     */
    private JavaPairRDD<Cell, Iterable<Vector>> getDenseCells(JavaPairRDD<Cell, Iterable<Vector>> allCells) {
        JavaPairRDD<Cell, Iterable<Vector>> denseCells = allCells
            .mapValues(v -> {
                int n = 0;
                Iterator<Vector> i = v.iterator();

                for (; i.hasNext(); i.next(), n++);
                return new Tuple2<>(n, v);
            })
            .filter(p -> p._2()._1() >= minPts)
            .mapValues(v -> v._2())
            .cache();
        
        return denseCells;
    }

    /**
     * Returns the core points contained in each cell.
     * 
     * @param allCells The PairRDD representing the 2D grid.
     * @param denseCells The PairRDD containing only the dense cells.
     * @param nonDenseCells The PairRDD containing only the non-dense cells.
     * @return A PairRDD containing the list of core points for each cell.
     */
    private JavaPairRDD<Cell, Iterable<Vector>> findCoreCells(JavaPairRDD<Cell, Iterable<Vector>> allCells, JavaPairRDD<Cell, Iterable<Vector>> denseCells, JavaPairRDD<Cell, Iterable<Vector>> nonDenseCells) {
        /* List points to check for every cell */
        JavaPairRDD<Cell, Iterable<Vector>> pointsToCheck = allCells
            .flatMapToPair(p -> {
                List<Tuple2<Cell, Vector>> tuples = new ArrayList<>();

                for (int i = p._1().getxPos() - 2; i <= p._1().getxPos() + 2; i++) {
                    for (int j = p._1().getyPos() - 2; j <= p._1().getyPos() + 2; j++) {
                        if ((i == p._1().getxPos() - 2 && j == p._1().getyPos() - 2) || 
                            (i == p._1().getxPos() - 2 && j == p._1().getyPos() + 2) || 
                            (i == p._1().getxPos() + 2 && j == p._1().getyPos() - 2) ||
                            (i == p._1().getxPos() + 2 && j == p._1().getyPos() + 2))
                            continue;
                        
                        for (Vector v : p._2())
                            tuples.add(new Tuple2<>(new Cell(i, j), v));
                    }
                }

                return tuples.iterator();
            })
            .groupByKey();
        
        /* Get core points from non-dense cells */
        JavaPairRDD<Cell, Iterable<Vector>> partiallyCoreCells = nonDenseCells
            .join(pointsToCheck)
            .flatMapToPair(p -> {
                Iterable<Vector> cellPoints = p._2()._1();
                Iterable<Vector> otherPoints = p._2()._2();
                List<Vector> corePoints = new ArrayList<>();
                List<Tuple2<Cell, Iterable<Vector>>> cellOrNot = new ArrayList<>();

                /* For all points in the current cell ... */
                for (Vector v1 : cellPoints) {
                    int neighbors = 0;

                    /* ... count all points in reachable distance from neighboring cells ... */
                    for (Vector v2 : otherPoints) {
                        if (distance(v1, v2) < eps)
                            neighbors++;
                    }

                    /* ... and identify a core point if they are at least minPts */
                    if (neighbors >= minPts)
                        corePoints.add(v1);
                }

                /* If the cell contains at least a core point, emit a pair */
                if (corePoints.size() > 0)
                    cellOrNot.add(new Tuple2<>(p._1(), corePoints));
                
                return cellOrNot.iterator();
            });

        return denseCells.union(partiallyCoreCells);
    }

    /**
     * Returns the outliers.
     * 
     * @param coreCells The PairRDD containing only the core cells.
     * @param nonCoreCells The PairRDD containin only the non-core cells.
     * @return An RDD containing all the outliers.
     */
    private JavaRDD<Vector> findOutliers(JavaPairRDD<Cell, Iterable<Vector>> coreCells, JavaPairRDD<Cell, Iterable<Vector>> nonCoreCells) {
        /* List points to check for every cell */
        JavaPairRDD<Cell, Iterable<Vector>> pointsToCheck = coreCells
            .flatMapToPair(p -> {
                List<Tuple2<Cell, Vector>> tuples = new ArrayList<>();

                for (int i = p._1().getxPos() - 2; i <= p._1().getxPos() + 2; i++) {
                    for (int j = p._1().getyPos() - 2; j <= p._1().getyPos() + 2; j++) {
                        if ((i == p._1().getxPos() - 2 && j == p._1().getyPos() - 2) || 
                            (i == p._1().getxPos() - 2 && j == p._1().getyPos() + 2) || 
                            (i == p._1().getxPos() + 2 && j == p._1().getyPos() - 2) ||
                            (i == p._1().getxPos() + 2 && j == p._1().getyPos() + 2))
                            continue;
                        
                        for (Vector v : p._2())
                            tuples.add(new Tuple2<>(new Cell(i, j), v));
                    }
                }

                return tuples.iterator();
            })
            .groupByKey();
        
        /* Get the list of outliers */
        JavaRDD<Vector> outliers = nonCoreCells
            .leftOuterJoin(pointsToCheck)
            .flatMap(p -> {
                /* If there is no neighbor, then all are outliers */
                if (!p._2()._2().isPresent())
                    return p._2()._1().iterator();
                
                Iterable<Vector> cellPoints = p._2()._1();
                Iterable<Vector> otherPoints = p._2()._2().get();
                List<Vector> points = new ArrayList<>();

                /* Otherwise, for all points in the current cell ... */
                for (Vector v1 : cellPoints) {
                    boolean outlier = true;

                    /* ... check if it is close to any reachable core point ... */
                    for (Vector v2 : otherPoints) {
                        if (distance(v1, v2) < eps) {
                            outlier = false;
                            break;
                        }
                    }

                    /* ... and, in case there is none, count it as an outlier */
                    if (outlier)
                        points.add(v1);
                }

                return points.iterator();
            });
        
        return outliers;
    }

}