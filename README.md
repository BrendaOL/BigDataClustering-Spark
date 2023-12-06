# BigDataClustering-Spark
Big Data Processing, implementing a bisecting k-means analysis in Spark.
This project is a practical assignment to implement the bisecting k-means algorithm for cluster detection in Spark and use it on a large dataset of NYC taxi trips that include all of the pickup points of more than 173 million individual trips[^1]. The goal is to cluster all pickup points to determine the ideal starting positions for the taxis.

### _This assignment is part of Cloud Computing and BigData course (2022) at the Vrije Universiteit Brussel, under the supervision of Prof. De Koster._
---
Bisecting k-means is a hierarchical variation of k-means clustering, therefore, the k-means algorithm will be developed first. The Tools and libraries used are:
- Development environment: IntelliJ IDEA
- Scala Version: 2.12.5
- Spark: 3.0.1
- JDK: 1.8

The effectiveness of bisecting K-means depends on the data set and the problem at hand. In some cases, the traditional K-means method may work just as well or even better.

Observations:

In terms of runtime, clustering a set of 10 using K-means typically takes between 8 to 15 seconds, whereas Bisecting K-means extends from 7 to 10 minutes. However, K-means results exhibit variability across each iteration, whereas Bisecting K-means consistently avoids this limitation. Bisecting K-means is preferred.

[^1]: https://www.andresmh.com/nyctaxitrips/
