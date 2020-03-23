package it.polito.s256654.thesis.algorithm.parallel;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.broadcast.Broadcast;

import it.polito.s256654.thesis.structures.Cell;
import it.polito.s256654.thesis.structures.CellMap;
import it.polito.s256654.thesis.structures.Vector;
import scala.Tuple2;

public class BroadcastOutlierDetector extends ParallelOutlierDetector {
    
    private static final long serialVersionUID = 1L;

    public BroadcastOutlierDetector(int dim, double eps, int minPts) {
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

        /* Collect pointsToCheck */
        Map<Cell, Iterable<Tuple2<Cell, Vector>>> pointsToCheckLocal = new HashMap<>();
        pointsToCheckLocal.putAll(pointsToCheck.groupByKey().collectAsMap());

        /* Broadcast pointsToCheck */
        Broadcast<Map<Cell, Iterable<Tuple2<Cell, Vector>>>> pointsToCheckBc = sc.broadcast(pointsToCheckLocal);

        /* Perform a broadcast join */
        JavaPairRDD<Cell, Vector> partiallyCoreCells = allCells
            .flatMapToPair(p -> {
                List<Tuple2<Tuple2<Cell, Vector>, Long>> joinedTuples = new ArrayList<>();

                if (pointsToCheckBc.value().containsKey(p._1())) {
                    for (Tuple2<Cell, Vector> p2 : pointsToCheckBc.value().get(p._1())) {
                        /* Check distance between points */
                        double d = p._2().distanceTo(p2._2());

                        /* Emit a pair ((cell, point), distance < eps) */
                        joinedTuples.add(new Tuple2<>(p2, d < eps ? 1L : 0L));
                    }
                }

                return joinedTuples.iterator();
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
        
        /* Collect pointsToCheck */
        Map<Cell, Iterable<Tuple2<Cell, Vector>>> pointsToCheckLocal = new HashMap<>();
        pointsToCheckLocal.putAll(pointsToCheck.groupByKey().collectAsMap());

        /* Broadcast pointsToCheck */
        Broadcast<Map<Cell, Iterable<Tuple2<Cell, Vector>>>> pointsToCheckBc = sc.broadcast(pointsToCheckLocal);
        
        /* Get the list of outliers */
        JavaPairRDD<Cell, Vector> outliersWithCoreNeighbors = coreCells
            .flatMapToPair(p -> {
                List<Tuple2<Tuple2<Cell, Vector>, Boolean>> joinedTuples = new ArrayList<>();

                if (pointsToCheckBc.value().containsKey(p._1())) {
                    for (Tuple2<Cell, Vector> p2 : pointsToCheckBc.value().get(p._1())) {
                        /* Check distance between points */
                        double d = p._2().distanceTo(p2._2());

                        /* Emit a pair ((cell, point), distance >= eps) */
                        joinedTuples.add(new Tuple2<>(p2, d >= eps));
                    }
                }

                return joinedTuples.iterator();
            })
            .reduceByKey((v1, v2) -> v1 && v2)              /* Combine information from all points */
            .filter(p -> p._2())                            /* Filter outliers */
            .mapToPair(p -> p._1());                        /* Map to the original vectors */
        
        return outliersWithCoreNeighbors.union(outliersWithNoCoreNeighbors);
    }

}