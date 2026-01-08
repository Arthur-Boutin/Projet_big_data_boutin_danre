# Real Estate Big Data Pipeline
[![Ask DeepWiki](https://devin.ai/assets/askdeepwiki.png)](https://deepwiki.com/Arthur-Boutin/Projet_big_data_boutin_danre)

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

## How to Run

This project is designed to be run with Docker and Docker Compose.

### Prerequisites

-   Docker
-   Docker Compose

### Setup

1.  **Clone the repository:**
    ```bash
    git clone https://github.com/arthur-boutin/projet_big_data_boutin_danre.git
    cd projet_big_data_boutin_danre
    ```

2.  **Set Airflow User ID (Linux/macOS):**
    To avoid permission issues with files created by Airflow, set the `AIRFLOW_UID` environment variable. You can add this to your shell profile (`.bashrc`, `.zshrc`) or run it in your current terminal session.
    ```bash
    echo "AIRFLOW_UID=$(id -u)" > .env
    ```

3.  **Build and start the services:**
    ```bash
    docker-compose up -d --build
    ```
    This command will build the custom Airflow image (with PySpark and Java dependencies), and start all services (Airflow, Elasticsearch, Kibana, Postgres, Redis) in the background.

### Accessing Services

-   **Airflow UI**: `http://localhost:8080`
    -   Login with username `airflow` and password `airflow`.
-   **Kibana**: `http://localhost:5601`
    -   Navigate here to create dashboards and explore the indexed data.
-   **Elasticsearch API**: `http://localhost:9200`

### Running the Pipeline

1.  Open the Airflow UI at `http://localhost:8080`.
2.  Find the `immobilier_big_data_pipeline` DAG on the main dashboard.
3.  Click the toggle button to un-pause the DAG.
4.  To start a run immediately, click the "play" button on the right side of the DAG's entry.

You can then monitor the progress of the pipeline in the "Grid" or "Graph" view. Once the indexing tasks are complete, the data will be available in Kibana. You may need to create index patterns in Kibana (e.g., `usage-*`, `gov-*`, `lbc-*`) to start visualizing the data.