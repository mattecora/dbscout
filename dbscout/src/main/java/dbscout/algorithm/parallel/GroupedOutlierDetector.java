package dbscout.algorithm.parallel;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.broadcast.Broadcast;

import dbscout.structures.Cell;
import dbscout.structures.CellMap;
import dbscout.structures.Vector;
import scala.Tuple2;

public class GroupedOutlierDetector extends ParallelOutlierDetector {
    
    private static final long serialVersionUID = 1L;

    public GroupedOutlierDetector(int dim, double eps, int minPts) {
        super(dim, eps, minPts);
    }

    @Override
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
        
        
        /* Get core points from non-dense cells */
        JavaPairRDD<Cell, Vector> partiallyCoreCells = allCells
            .groupByKey()                                   /* Group for better join performance */
            .join(pointsToCheck)                            /* Join with the points to be checked */
            .mapToPair(p -> {
                long neighborsCount = 0;

                for (Vector v : p._2()._1()) {
                    /* Check distance between points */
                    double d = v.distanceTo(p._2()._2()._2());

                    /* Increment neighbors count if distance < eps */
                    if (d < eps) neighborsCount++;

                    /* Break loop if at least minPts neighbors are present */
                    if (neighborsCount >= minPts) break;
                }

                /* Emit a pair ((cell, point), number of neighbors) */
                return new Tuple2<>(p._2()._2(), neighborsCount);
            })
            .reduceByKey((v1, v2) -> v1 + v2)               /* Count points with distance < eps */
            .filter(p -> p._2() >= minPts)                  /* Filter core points */
            .mapToPair(p -> p._1());                        /* Emit a pair (cell, point) */

        return partiallyCoreCells;
    }

    @Override
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
        
        /* Get the list of outliers */
        JavaPairRDD<Cell, Vector> outliersWithCoreNeighbors = coreCells
            .groupByKey()                           /* Group for better join performance */
            .join(pointsToCheck)                    /* Join with the points to be checked */
            .mapToPair(p -> {
                boolean isOutlier = true;

                for (Vector v : p._2()._1()) {
                    /* Check distance between points */
                    double d = v.distanceTo(p._2()._2()._2());

                    /* If distance < eps, then point is not an outlier */
                    if (d < eps) {
                        isOutlier = false;
                        break;
                    }
                }

                /* Emit a pair ((cell, point), outlier or not) */
                return new Tuple2<>(p._2()._2(), isOutlier);
            })
            .reduceByKey((v1, v2) -> v1 && v2)              /* Combine information from all points */
            .filter(p -> p._2())                            /* Filter outliers */
            .mapToPair(p -> p._1());                        /* Map to the original vectors */
        
        return outliersWithCoreNeighbors.union(outliersWithNoCoreNeighbors);
    }

}