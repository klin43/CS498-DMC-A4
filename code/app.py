from flask import Flask, request, jsonify
from neo4j import GraphDatabase
from pyspark.sql import SparkSession
import pyspark.sql.functions as F

app = Flask(__name__)
app.json.sort_keys = False

CSV = "taxi_trips_clean.csv"
URL = "bolt://34.171.203.153:7687"
PASSWORD = "Black123"
driver = GraphDatabase.driver(URL, auth=("neo4j", PASSWORD))

spark = SparkSession.builder.appName("TaxiAPI").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

### Part 1 ###
# e1 - graph summary
@app.route("/graph-summary")
def graph_summary():
    with driver.session() as session:
        driver_count = session.run("MATCH (d:Driver) RETURN count(d) AS c").single()["c"]
        company_count = session.run("MATCH (c:Company) RETURN count(c) AS c").single()["c"]
        area_count = session.run("MATCH (a:Area) RETURN count(a) AS c").single()["c"]
        trip_count = session.run("MATCH ()-[t:TRIP]->() RETURN count(t) AS c").single()["c"]

    return jsonify({
        "driver_count": driver_count,
        "company_count": company_count,
        "area_count": area_count,
        "trip_count": trip_count})
        
# e2 - top companies by trip volume
@app.route("/top-companies")
def top_companies():
    n = int(request.args.get("n", 5))

    query = """ 
        MATCH (d:Driver)-[:WORKS_FOR]->(c:Company)
        MATCH (d)-[:TRIP]->()
        RETURN c.name AS name, count(*) AS trip_count
        ORDER BY trip_count DESC
        LIMIT $n
    """

    with driver.session() as session:
        result = session.run(query, n=n)
        companies = [dict(r) for r in result]

    return jsonify({"companies": companies})

# e3 - high fare trips to an area
@app.route("/high-fare-trips")
def high_fare_trips():
    area_id = int(request.args.get("area_id"))
    min_fare = float(request.args.get("min_fare"))

    query = """
        MATCH (d:Driver)-[t:TRIP]->(a:Area)
        WHERE a.area_id = $area_id AND t.fare > $min_fare
        RETURN t.trip_id AS trip_id, t.fare AS fare, d.driver_id AS driver_id
        ORDER BY t.fare DESC
    """

    with driver.session() as session:
        result = session.run(query, area_id=area_id, min_fare=min_fare)
        trips = [dict(r) for r in result]

    return jsonify({"trips": trips})

# e4 - co-area drivers
@app.route("/co-area-drivers")
def co_area_drivers():
    driver_id = request.args.get("driver_id")
    
    query = """
        MATCH (d1:Driver {driver_id: $driver_id})-[:TRIP]->(a:Area)<-[:TRIP]-(d2:Driver)
        WHERE d1 <> d2
        RETURN d2.driver_id AS driver_id, COUNT(DISTINCT a) AS shared_areas
        ORDER BY shared_areas DESC
    """

    with driver.session() as session:
        result = session.run(query, driver_id=driver_id)
        drivers = [dict(r) for r in result]

    return jsonify({"co_area_drivers": drivers})
    
# e5 - avg fare by company
@app.route("/avg-fare-by-company")
def avg_fare_by_company():
    query = """
        MATCH (d:Driver)-[:WORKS_FOR]->(c:Company)
        MATCH (d)-[t:TRIP]->()
        RETURN c.name AS name, ROUND(AVG(t.fare), 2) AS avg_fare
        ORDER BY avg_fare DESC
    """

    with driver.session() as session:
        result = session.run(query)
        companies = [dict(r) for r in result]

    return jsonify({"companies": companies})

### PART 2.2 ###
# e6 - area stats
@app.route("/area-stats")
def area_stats():
    area_id = int(request.args.get("area_id"))
    df = spark.read.csv(CSV, header=True, inferSchema=True)
    
    result = df.filter(F.col("dropoff_area") == area_id) \
        .agg(F.count("*").alias("trip_count"),
            F.round(F.avg("fare"), 2).alias("avg_fare"),
            F.round(F.avg("trip_seconds"), 0).alias("avg_trip_seconds")
        ).collect()[0]

    return jsonify({
        "area_id": area_id,
        "trip_count": int(result["trip_count"]),
        "avg_fare": float(result["avg_fare"]),
        "avg_trip_seconds": int(result["avg_trip_seconds"])
    })
    
# e7 - top pickup areas
@app.route("/top-pickup-areas")
def top_pickup_areas():
    n = int(request.args.get("n", 5))
    df = spark.read.csv(CSV, header=True, inferSchema=True)
    
    result = df.groupBy("pickup_area") \
        .agg(F.count("*").alias("trip_count")) \
        .orderBy(F.col("trip_count").desc()) \
        .limit(n) \
        .collect()
        
    areas = [{"pickup_area": int(r["pickup_area"]), "trip_count": r["trip_count"]} for r in result]

    return jsonify({"areas": areas})
    
# e8 - company compare
@app.route("/company-compare")
def company_compare():
    c1 = request.args.get("company1")
    c2 = request.args.get("company2")
    
    df = spark.read.csv(CSV, header=True, inferSchema=True)
    df = df.withColumn("fare_per_minute", F.col("fare") / (F.col("trip_seconds") / 60.0))
    df.createOrReplaceTempView("trips")
    
    result = spark.sql(f"""
        SELECT company,
               COUNT(*) AS trip_count,
               ROUND(AVG(fare), 2) AS avg_fare,
               ROUND(AVG(fare_per_minute), 2) AS avg_fare_per_minute,
               ROUND(AVG(trip_seconds), 0) AS avg_trip_seconds
        FROM trips
        WHERE company IN ('{c1}', '{c2}')
        GROUP BY company
        ORDER BY trip_count DESC
    """).collect()

    if len(result) < 2:
        return jsonify({"error": "one or more companies not found"}), 400

    return jsonify({"comparison": [r.asDict() for r in result]})
    

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)
