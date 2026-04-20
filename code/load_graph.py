from neo4j import GraphDatabase
import pandas as pd

URL = "bolt://34.171.203.153:7687"
PASSWORD = "Black123"
df = pd.read_csv("taxi_trips_clean.csv")

driver = GraphDatabase.driver(URL, auth=("neo4j", PASSWORD))

def create_graph(transaction, row):
    transaction.run("""
        MERGE (d:Driver {driver_id: $driver_id})
        MERGE (c:Company {name: $company})
        MERGE (a:Area {area_id: $dropoff_area})
        MERGE (d)-[:WORKS_FOR]->(c)

        CREATE (d)-[:TRIP {
            trip_id: $trip_id,
            fare: $fare, 
            trip_seconds: $trip_seconds
            }]->(a)
    """,
    trip_id=row["trip_id"],
    driver_id=row["driver_id"],
    company=row["company"],
    pickup_area=row["pickup_area"],
    dropoff_area=int(row["dropoff_area"]),
    fare=float(row["fare"]),
    trip_seconds=int(row["trip_seconds"])
    )

def main():
    with driver.session() as session:
        for _, row in df.iterrows():
            session.execute_write(create_graph, row)

    driver.close()
    print("Load graph done!")
    
if __name__ == "__main__":
    main()
