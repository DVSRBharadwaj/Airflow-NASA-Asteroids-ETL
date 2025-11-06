from airflow import DAG
#from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.providers.http.operators.http import HttpOperator
from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook
#from airflow.utils.dates import days_ago
from datetime import datetime, timedelta  # WORKS
from datetime import datetime
import json

# Define the DAG
with DAG(
    dag_id='nasa_asteroids_neows_postgres',
    start_date=datetime(2025, 11, 1),
    schedule='@daily',
    catchup=False,
    tags=['nasa', 'asteroids', 'etl'],
    default_args={'retries': 1}
) as dag:

    # Step 1: Create the asteroids table if not exists
    @task
    def create_table():
        postgres_hook = PostgresHook(postgres_conn_id='my_postgres_connection')

        create_table_query = """
        CREATE TABLE IF NOT EXISTS asteroids_neows (
            id SERIAL PRIMARY KEY,
            asteroid_name VARCHAR(255) NOT NULL,
            nasa_jpl_url TEXT,
            is_hazardous BOOLEAN,
            diameter_min_meters FLOAT,
            diameter_max_meters FLOAT,
            close_approach_date DATE,
            relative_velocity_kmh FLOAT,
            miss_distance_km FLOAT,
            orbiting_body VARCHAR(50),
            ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """
        postgres_hook.run(create_table_query)


    # Step 2: Extract today's asteroid data from NASA NeoWs
    extract_asteroids = HttpOperator(
        task_id='extract_asteroids',
        http_conn_id='nasa_api',
        endpoint='neo/rest/v1/feed/today',
        method='GET',
        data={
            "api_key": "{{ conn.nasa_api.extra_dejson.api_key }}",
            "detailed": "false"
        },
        response_filter=lambda response: response.json(),
        log_response=True
    )


    # Step 3: Transform - Flatten nested asteroid data
    @task(multiple_outputs=False)
    def transform_asteroids(response):
        records = []
        neo_data = response.get("near_earth_objects", {})

        # NeoWs returns dict with date as key: {"2025-11-06": [...]}
        for date_str, asteroids in neo_data.items():
            approach_date = datetime.strptime(date_str, "%Y-%m-%d").date()

            for ast in asteroids:
                # Safely extract close approach data (always first element)
                cad = ast.get("close_approach_data", [{}])[0]

                record = {
                    "asteroid_name": ast.get("name", "Unknown"),
                    "nasa_jpl_url": ast.get("nasa_jpl_url"),
                    "is_hazardous": ast.get("is_potentially_hazardous_asteroid", False),
                    "diameter_min_meters": ast.get("estimated_diameter", {})
                                                .get("meters", {})
                                                .get("estimated_diameter_min"),
                    "diameter_max_meters": ast.get("estimated_diameter", {})
                                                .get("meters", {})
                                                .get("estimated_diameter_max"),
                    "close_approach_date": approach_date,
                    "relative_velocity_kmh": float(cad.get("relative_velocity", {})
                                                    .get("kilometers_per_hour", 0) or 0),
                    "miss_distance_km": float(cad.get("miss_distance", {})
                                               .get("kilometers", 0) or 0),
                    "orbiting_body": cad.get("orbiting_body")
                }
                records.append(record)

        return records  # List of dicts


    # Step 4: Load each asteroid record into Postgres
    @task
    def load_asteroids(records):
        if not records:
            return "No asteroid data to load."

        postgres_hook = PostgresHook(postgres_conn_id='my_postgres_connection')

        insert_query = """
        INSERT INTO asteroids_neows (
            asteroid_name, nasa_jpl_url, is_hazardous,
            diameter_min_meters, diameter_max_meters,
            close_approach_date, relative_velocity_kmh,
            miss_distance_km, orbiting_body
        ) VALUES (
            %(asteroid_name)s, %(nasa_jpl_url)s, %(is_hazardous)s,
            %(diameter_min_meters)s, %(diameter_max_meters)s,
            %(close_approach_date)s, %(relative_velocity_kmh)s,
            %(miss_distance_km)s, %(orbiting_body)s
        );
        """

        for record in records:
            postgres_hook.run(insert_query, parameters=record)

        return f"Successfully loaded {len(records)} asteroids into Postgres!"



    # Task Dependencies
    create_table_task = create_table()
    extracted_data = extract_asteroids.output
    transformed_data = transform_asteroids(extracted_data)
    load_task = load_asteroids(transformed_data)

    create_table_task >> extract_asteroids >> transformed_data >> load_task