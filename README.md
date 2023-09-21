# engineeringForAI

Task 1: data cleaning [40% of this assignment marks].
This has two parts:

Task 1.1 remove '0 distance' and 'no passengers' records:
you are asked to remove trips where trip_distance == 0, as those will compromise the rest of the analysis.

You will then also remove remove trips where passenger_count == 0, however we want to retain records where total_amount > 0 (as these may be significant as the taxi may have carried some parcel, for example). Thus you need to implement a logic condition to filter the dataset accordingly.

Task 1.2 remove outliers using the modified z-score
In class we have presented an anomaly detection method based on z-score, which however uses the median rather than mean.

Here is the relevant presentation: Anomaly-detection.pdf Download Anomaly-detection.pdf 

We also remarked that calculating the median for a large dataset is expensive, as it requires a full sort. With this in mind, your task is to remove records where values in columns `total_amount`, `trip_distance` are outliers.

To achieve this, you will:

replace the median calculation with an approximation, provided by the percentile_approx function available in Spark:. Docs https://spark.apache.org/docs/3.2.0/api/python/reference/api/pyspark.sql.functions.percentile_approx.html?highlight=percentile#pyspark.sql.functions.percentile_approxLinks to an external site.
Links to an external site.once you have computed the z-scores for each of the columns in each record, select a threshold, above which the value is declared to be an outlier. The choice of threshold is not critical, as we are simply interested in performance here.  For the sake of this exercise, you will just use the constant 3.5 as your threshold, i.e., z-score > 3.5 is an outlier.
Task 2: add  "engineered features" to the datasets [30% of this assignment marks]
Task 2.1: add the names of start/end zones for each trip to the trips dataframe
NYC is divided into some 260 zones, each has an ID and a descriptive name. The table is linked under "Taxi Zone Maps and Lookup Tables" here: https://s3.amazonaws.com/nyc-tlc/misc/taxi+_zone_lookup.csvLinks to an external site.. A copy is available for you on Databricks so no need to donwload it yourself. It looks like this:

LocationID:integer
Borough:string
Zone:string
service_zone:string
Each taxi trip record comes with two IDs: PULocationID and DOLocationID, which specify the source and end zone of the trip, respectively.
Your first task is to add two columns to your trips dataframe, with the `Zone` corresponding to each of the two IDs.
The result is a new dataframe with the two new columns added.
Again, you aim for an efficient Spark implementation.
Hint: use UDFs, as presented in class. See also the support documentation below.
Task 2.2: add a calculated column to the trips dataframe
We are interested in analysing the "profitability" of each zone as a source of a taxi trip. To achieve this, you are asked to add  column unit_profitabilty to the trips dataframe, by calculating total_amount / trip_distance for each record.
Task 3: Build zones graph and Rank zones according to their traffic, passengers volume, profitability [30% of this assignment marks].
Task 3.1: build a graph data structure to represent aggregated data about trips between any two zones.
This graph will have one node for each zone, and one edge connecting each pair of zones and carrying aggregate information about all trips between those zones.

For example, zones Z1 and Z2 are connected by two edges: edge Z1 --> Z2 carries aggregate data about all trips that originated in Z1 and ended in Z2, and edge Z2 --> Z1 carries aggregate data about all trips that originated in Z2 and ended in Z1. The aggregate information includes the following:

average_unit_profit. This is just the average of unit_profitabilty as calculated for each trip in the previous task
trips_count. This is just a count() of all trips from Z1 to Z2
total_passengers. This is the sum of Passenger_count across all trips
This graph can be represented as a new dataframe, with schema:

[PULocationID, DOLocationID, average_unit_profit, trips_count, total_passengers ]

hint: the groupby() operator produces a pyspark.sql.GroupedData structure. You can then calculate multiple aggregations from this using pyspark.sql.GroupedData.agg(). Docs here:

https://spark.apache.org/docs/3.2.0/api/python/reference/pyspark.pandas/api/pyspark.pandas.DataFrame.groupby.htmlLinks to an external site.
https://spark.apache.org/docs/3.2.0/api/python/reference/api/pyspark.sql.GroupedData.agg.htmlLinks to an external site.
Task 3.2
For each of the calculated measures, report the top-10 zones using their plain names you dereferenced in the previous step, not the codes. Note that this requires ranking the nodes in different orders. Specifically, you need to calculate the following further aggregations:

the total number of trips originating from Z. This is simply the sum of trips_count over all outgoing edges for Z, i.e., edges of the form Z -> *

the average profitability of a zone. This is the average of all average_unit_profit over all outgoing edges from Z.

The total passenger volume measured as the sum of total_passengers carried in trips that originate from Z
