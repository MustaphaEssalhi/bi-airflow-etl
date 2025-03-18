# BI Data Pipeline with Apache Airflow

## Project Overview
This project sets up an ETL (Extract, Transform, Load) pipeline using Apache Airflow to extract data from MySQL, transform it, and load it into PostgreSQL for BI (Business Intelligence) visualization.

## Directory Structure
```
├── config/                 # Configuration files
├── dags/                   # Airflow DAGs (Directed Acyclic Graphs)
│   ├── my-bi.py            # Main DAG for the ETL pipeline
├── docker-compose.yml       # Docker configuration for running Airflow
├── export/                 # Exported data storage
│   ├── export-data-ps.py    # Script for exporting data to PostgreSQL
│   ├── exported_tables/     # Directory containing exported tables
├── import/                 # Import-related files
│   ├── import-access.py     # Script for importing Microsoft Access data
├── logs/                    # Airflow logs
├── plugins/                 # Custom Airflow plugins
├── requirements.txt         # Python dependencies
```

## Setup & Installation
### Prerequisites
Ensure you have the following installed:
- Docker & Docker Compose
- Python 3.x
- Apache Airflow

### Steps to Run the Project
1. Clone the repository:
   ```bash
   git clone <your-repo-url>
   cd bi
   ```
2. Set up Python dependencies:
   ```bash
   pip install -r requirements.txt
   ```
3. Start Airflow using Docker Compose:
   ```bash
   docker-compose up -d
   ```
4. Access the Airflow UI at: [http://localhost:8080](http://localhost:8080)
5. Trigger the DAG manually from the UI or wait for scheduled execution.

## DAG Overview
The main DAG (`my-bi.py`) performs the following steps:
1. Extract data from MySQL.
2. Transform the extracted data.
3. Load the transformed data into PostgreSQL.
4. Store exported data for visualization.

## Download Orion Database
The `orion.mdb` file is not included in the repository. You can download it from the following link:
[Download orion.mdb](<link>)

Place the downloaded `orion.mdb` file inside the `import/` directory before running the pipeline.

## Troubleshooting
- Check Airflow logs in the `logs/` directory if any issues occur.
- Ensure all required environment variables for database connections are set up.
- Restart Airflow if changes are not reflected:
  ```bash
  docker-compose restart
  ```

## Future Enhancements
- Implement monitoring and alerting for DAG failures.
- Optimize data transformations for better performance.
- Integrate with BI tools like Tableau or Power BI.

## License
This project is licensed under the MIT License.

