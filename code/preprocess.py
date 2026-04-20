from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# 1 - initialize spark session
spark = SparkSession.builder.appName("TaxiTrips").getOrCreate()

# 2 - read csv
df = spark.read.csv("taxi_trips_clean.csv", header=True, inferSchema=True)

# 3
df = df.withColumn("fare_per_minute", col("fare") / (col("trip_seconds") / 60.0))

# 4
df.createOrReplaceTempView("trips")

# 5
result = spark.sql("""
    SELECT 
        company, 
        COUNT(*) AS trip_count, 
        ROUND(AVG(fare), 2) AS avg_fare,
        ROUND(AVG(fare_per_minute), 2) AS avg_fare_per_minute
    FROM trips
    GROUP BY company
    ORDER BY trip_count DESC
""")

# 6
result.write.json("processed_data/", mode="overwrite")
print("preprocess done!")
spark.stop()
