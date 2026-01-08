# Real Estate Big Data Pipeline

This project implements a complete data engineering pipeline to collect, process, and analyze French real estate data. It fetches property transaction data from the official government source (DVF) and current real estate listings from Leboncoin. The data is processed using PySpark, stored in a structured data lake, and indexed into Elasticsearch for analysis and visualization with Kibana. The entire workflow is orchestrated by Apache Airflow.

## Architecture

The project is fully containerized using Docker and Docker Compose, setting up an integrated environment with the following components:

-   **Orchestration:** Apache Airflow manages and schedules the entire data pipeline.
-   **Data Processing:** PySpark is used to perform transformations, aggregations, and join data from different sources.
-   **Data Storage:**
    -   **Data Lake:** A multi-layer data lake (`raw`, `formatted`, `usage`) stores data in different processing stages, using Parquet for the processed layers.
    -   **Search & Analytics:** Elasticsearch is used to index all data layers, enabling fast and complex queries.
-   **Visualization:** Kibana provides a user interface to explore and visualize the data indexed in Elasticsearch.
-   **Data Sources:**
    -   **DVF (Demandes de Valeurs Foncières):** Official data on property transactions from `data.gouv.fr`.
    -   **Leboncoin:** Current real estate listings for sale in the Paris area, scraped using an included client library.

  - *Replace with a real diagram if available, but for now, we follow the "no placeholder" rule.*

## Pipeline Workflow

The main Airflow DAG, `immobilier_big_data_pipeline`, orchestrates the following sequence of tasks:

1.  **Fetch DVF Data**: Downloads the latest full dataset of 2025 property transactions from `data.gouv.fr` and stores it in the `raw` layer of the data lake.
2.  **Transform DVF Data**: Converts the raw DVF CSV file into a cleaned, optimized Parquet file in the `formatted` layer.
3.  **Fetch Leboncoin Data**: Scrapes the latest real estate listings for Paris from Leboncoin. The process is incremental, using a state file to fetch only new ads since the last run. Raw JSON data is stored in the `raw` layer.
4.  **Transform Leboncoin Data**: Processes the raw JSON listings, cleans the data (handling types, duplicates), and saves it as a Parquet file in the `formatted` layer.
5.  **Compute Usage Layer (Spark)**: A Spark job reads the formatted DVF and Leboncoin data to create a `usage` layer. This job:
    -   Calculates market statistics from DVF data, such as the average price per m² per city (`market_analysis`).
    -   Enriches Leboncoin listings with this market data to identify potential investment opportunities.
6.  **Index to Elasticsearch**: Several tasks run in parallel to index the data into Elasticsearch, making it available for Kibana:
    -   `usage-opportunities`: Enriched Leboncoin ads.
    -   `usage-market-stats`: Aggregated market statistics from DVF.
    -   `gov-dvf` & `gov-dvf-paris`: Granular, formatted DVF transaction data.
    -   `lbc-annonces`: Formatted Leboncoin ads.

## 🚀 How to Run

This project is designed to be run with Docker. Everything is included (Airflow, Spark, Elasticsearch, Kibana, Scripts).

### Prerequisites
-   **Docker Desktop** installed and running.
-   **Git** installed.

### Installation & Setup

1.  **Clone the repository**:
    ```bash
    git clone https://github.com/Arthur-Boutin/Projet_big_data_boutin_danre.git
    cd Projet_big_data_boutin_danre
    ```

2.  **Fix Permissions (Linux/Mac only)**:
    *If you are on Windows, skip this step.*
    ```bash
    echo "AIRFLOW_UID=$(id -u)" > .env
    ```

3.  **Start the environment**:
    We will build the image (to install dependencies) and start the containers.
    ```bash
    docker-compose up -d --build
    ```
    *Wait a few minutes for everything to start (Healthcheck).*

### Accessing Services

-   **Airflow**: [http://localhost:8080](http://localhost:8080) (Log: `airflow` / `airflow`)
-   **Kibana**: [http://localhost:5601](http://localhost:5601)
-   **Elasticsearch**: [http://localhost:9200](http://localhost:9200)

### Running the Pipeline

1.  Go to Airflow (**127.0.0.1:8080**).
2.  In the DAGs list, activate (Toggle **ON**) the DAG `immobilier_big_data_pipeline`.
3.  Click the **Play** button (▶️) on the right to trigger it manually.
4.  Monitor progress in the **Grid** tab.

### Visualizing Data (Kibana)

Once the pipeline finishes (tasks turn green):
1.  Go to Kibana.
2.  **Stack Management** > **Data Views** > **Create data view**.
3.  Create `Usage Data` (pattern `usage-*`) and `DVF Paris` (pattern `gov-dvf-paris`).
4.  Go to **Discover** or **Maps** to explore!

---

## 🛠 Troubleshooting

-   **"No space left on device" error**: Docker needs space. Perform a cleanup: `docker system prune`.
-   **Pipeline fails on indexing**: Check that Elasticsearch is "Healthy" in Docker Desktop.
-   **Airflow doesn't load**: Wait 30-60 seconds after `docker-compose up`.