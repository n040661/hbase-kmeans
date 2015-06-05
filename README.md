# hbase-kmeans
Iterative k-means using HBase and Map Reduce
# Problem Statement
To cluster the Energy Efficiency dataset (http://archive.ics.uci.edu/ml/datasets/Energy+efficiency) using K-means on the HBase platform by iteratively running Map Reduce Jobs.
# Data Loading into Hbase
Performed by the DataLoader Class. This class imports the Energy Efficiency dataset (by treating the data as a 10 dimensional feature vector) into an HBase table called "data".The data is imported into 2 different column families: Area and Property.

# Steps for importing Data:

1) Delete any existing Data Table. 

2) Get the path of the local text file that contains the data from the program arguments (args[0]).

3) Store the number of clusters. This is also obtained from the program arguments (args[1]).

4) Use a Map Reduce Job to import the data into the "data" table. 

5) Also, load the first k rows as the initial cluster centers in a table called "center". 

# Steps to Run Kmeans

Basic Idea of the algorithm:

In the mapper phase, we emit <Nearest cluster number, data row>. The current cluster centers are obtained in the Setup phase of the Mapper. The Euclidean distance is computed to each cluster center and the cluster number that corresponds to the lowest cluster is obtained. 

In the reducer phase, we compute the mean of all the data points that have been assigned to that cluster. We update the "center" table with the new cluster centers. 

When do we stop the iterations?

We stop when the cluster centers do not change by much across an iteration. Since, this is usually run on a distributed environment, we need to use Hadoop counters to figure out when to stop. We take a counter and set its value to 0 in the reducer. At the end of the reducer phase, we check if we need to iterate again. If we need to iterate again, we increment this counter value by 1. In the Job Runner class, we check the value of this counter after every job (Map+Reduce). If this value is 0, the counter in the Reducer has not incremented and we can stop iterating further. 

# Further work
There are a few changes that can be made to improve the performance of kmeans. This whole procedure can be run by having multiple random restarts where in each step, we execute the full kmeans algorithm from a different set of random initial points. We can then compute the best assignment of points by using the result that has the lowest sum of squares.  
