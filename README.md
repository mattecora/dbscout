# A density-based method for scalable outlier detection in large datasets

## Summary

DBSCAN is one of the most well-known algorithm in the field of density-based clustering, although its applicability to large datasets is generally disputed due to its high complexity. The aim of this work is to propose a new, parallel, Spark-based procedure for the sole purpose of anomaly detection, in a way which is coherent to the DBSCAN definition and suitable for the big data context. From a theoretical side, this algorithm is characterized by a worst-case performance boundary that depends linearly on the size of the dataset; in practical tests, it outperforms available solutions both in terms of result quality and overall scalability when the data grow large.

## Repository organization

The repository is organized in the following folders:

* `algcomp`: scripts to run reference algorithms for result comparison.
* `data`: data generation scripts.
* `newalg`: the code of the new algorithm.
* `utils`: Spark-based utility scripts.
* `visualization`: result visualization scripts.

## How to run

The code for the new algorithm is stored in the `newalg` folder, in the form of a Maven project. Compile using `mvn package` and run either through `java` or `spark-submit`.

Supported command-line options include:

* `--algClass`: the algorithm class (*required*). Available options:
  * `it.polito.s256654.thesis.algorithm.sequential.SequentialOutlierDetector`: the base version of the sequential algorithm.
  * `it.polito.s256654.thesis.algorithm.parallel.ParallelOutlierDetector`: the base version of the parallel algorithm.
  * `it.polito.s256654.thesis.algorithm.parallel.BroadcastOutlierDetector`: the broadcast join optimized version of the parallel algorithm.
  * `it.polito.s256654.thesis.algorithm.parallel.GroupedOutlierDetector`: the grouping before join optimized version of the parallel algorithm.
* `--dim`: the data dimensions (*required*).
* `--eps`: the value of the epsilon parameter (*required*).
* `--inputPath`: the input path (*required*).
* `--minPts`: the value of the minPts parameter (*required*).
* `--numPart`: the number of data partitions.
* `--outputPath`: the output path (*required*).
* `--stats`: print dataset statistics.

## References

Matteo Corain. *A density-based method for scalable outlier detection in large datasets*. Master's thesis, Politecnico di Torino and University of Illinois at Chicago, May 2020.