
# Data Pipeline Project

This project outlines a data pipeline that extracts data from the Amazon website, processes it using Apache Airflow, and stores it in Azure Data Gen2. The processed data is then further transformed using Azure Data Factory and analyzed using Azure Synapse Analytics, Databricks, and Tableau.

## Architecture

![Data Pipeline Architecture](https://github.com/manhzeff/Data-Engineering-and-Analyst-with-Amazon-Data/assets/104782892/48fcad68-e566-4edd-9821-9afedba1152a)

## Components

1. **Amazon Website**:
   - Source of raw data.

2. **Apache Airflow**:
   - Orchestrates the data extraction, transformation, and loading (ETL) processes.
   - Manages workflows and ensures data is processed in the correct sequence.
   - Docker is used to containerize Airflow for easy deployment and scalability.

3. **PostgreSQL**:
   - Database for storing intermediary data during the ETL process.

4. **Azure Data Gen2**:
   - Scalable data lake that stores the transformed data.
   - Acts as the central repository for all data processed by the pipeline.

5. **Azure Data Factory**:
   - Data integration service that automates data movement and transformation.
   - Orchestrates data flow from Azure Data Gen2 to downstream components.

6. **Databricks**:
   - Provides a unified analytics platform for big data and AI.
   - Used for further data transformation and machine learning model development.

7. **Azure Synapse Analytics**:
   - Data analytics service that brings together big data and data warehousing.
   - Used for advanced data analysis and reporting.

8. **Tableau**:
   - Data visualization tool used for creating interactive and shareable dashboards.
   - Provides insights from the processed data for business intelligence purposes.

## Workflow

1. **Data Extraction**:
   - Data is extracted from the Amazon website using scraping tools or APIs.
   - Extracted data is initially stored in PostgreSQL.

2. **ETL with Airflow**:
   - Apache Airflow orchestrates the ETL process, where data is cleaned, transformed, and loaded into Azure Data Gen2.
   - Docker containers ensure Airflow is easily scalable and deployable.

3. **Data Integration**:
   - Azure Data Factory automates the movement of data from Azure Data Gen2 to Databricks and Azure Synapse Analytics for further processing.

4. **Data Transformation and Analysis**:
   - Databricks performs complex data transformations and machine learning model training.
   - Azure Synapse Analytics provides a platform for deep data analysis and querying.

5. **Data Visualization**:
   - Tableau connects to Azure Synapse Analytics to visualize the processed data.
   - Interactive dashboards are created to provide insights for decision-making.

## Getting Started

To run this project, follow these steps:

1. **Clone the repository**:
   ```sh
   git clone https://github.com/yourusername/your-repo.git

### Set up Docker for Airflow

Refer to the [official documentation](https://airflow.apache.org/docs/apache-airflow/stable/start/docker.html) for setting up Apache Airflow with Docker.

### Configure PostgreSQL

Set up PostgreSQL database and update the connection details in the Airflow DAGs.

### Deploy to Azure

Follow the [Azure documentation](https://docs.microsoft.com/en-us/azure/data-factory/introduction) to set up Azure Data Factory, Data Gen2, Synapse Analytics, and Databricks.

### Visualize with Tableau

Connect Tableau to Azure Synapse Analytics and create your dashboards.

## Contributing

If you would like to contribute to this project, please fork the repository and submit a pull request with your changes.

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

