# README: Game Performance Data Pipeline

## Overview
This project demonstrates a complete data pipeline for ingesting, transforming, and validating game performance data. The pipeline involves the following key steps:

1. **Data Ingestion**: Loading CSV data into a PostgreSQL database.
2. **Data Transformation**: Using dbt (Data Build Tool) to transform the ingested data into aggregated insights.
3. **Data Quality Checks**: Implementing dbt tests to ensure the accuracy and integrity of the data.

---

## Setup Process

### Prerequisites
- **Python 3.8 or higher**
- **PostgreSQL database**
- **dbt (version 1.9.0)**
- Required Python packages:
  ```bash
  pip install pandas psycopg2 dbt-postgres
  ```

### Step 1: Clone the Repository
Clone this repository to your local machine:
```bash
git clone <repository_url>
cd <repository_directory>
```

### Step 2: Configure the Database
1. Set up a PostgreSQL database with the following details:
   - **Database Name**: `game_db`
   - **Schema**: `public`
2. Update the `dbt_project.yml` file with your PostgreSQL connection details:
   ```yaml
   profiles:
     game_project:
       target: dev
       outputs:
         dev:
           type: postgres
           host: localhost
           user: your_username
           password: your_password
           port: 5432
           dbname: game_db
           schema: public
   ```

### Step 3: Data Ingestion
Run the Python script to ingest the CSV file into the PostgreSQL database:
```bash
python scripts/load_csv_to_postgres.py
```
This script reads a CSV file (`data/game_performance.csv`) and loads it into the `game_performance_data` table.

---

## dbt Project Structure

```
repository/
│
├── data/
│   ├── Data_Engineer_Challenge_input.csv    # Full load CSV file
│   ├── incremental_data.csv                # Incremental load CSV file
│
├── scripts/
│   ├── pipeline.py                         # Main pipeline script
│   ├── load_csv_to_sflake.py               # Script for initial Snowflake ingestion
│
├── my_dbt_project/
│   ├── dbt_project.yml                     # dbt project configuration file
│   ├── macros/                             # Custom macros directory
│   │   ├── custom_positive_check.sql       # Custom test for positive values
│   │   └── custom_valid_date_format.sql    # Custom test for date format validation
│   ├── models/                             # dbt models directory
│   │   ├── game_performance_incremental.sql # Incremental model for GAME_PERFORMANCE_DATA
│   │   ├── daily_summary.sql               # Model for daily turnover and revenue summary
│   │   ├── revenue_by_egm_and_venue.sql    # Model for revenue aggregation by EGM and venue
│   │   ├── total_turnover_by_venue.sql     # Model for total turnover aggregation by venue
│   │   └── schema.yml                      # Source configuration and column-level tests
│
├── tests/
│   ├── custom_positive_check.sql           # Custom test definition
│   ├── custom_valid_date_format.sql        # Custom test definition
│
├── requirements.txt                        # Python dependencies for the project
│
└── README.md                               # Documentation for the project

```

---

## Running the Pipeline

### Step 1: Install Dependencies
Ensure all required dependencies are installed.

### Step 2: Run dbt Commands
1. **Initialize the dbt environment**:
   ```bash
   dbt init game_project
   ```

2. **Run dbt Models**:
   Build the transformation models:
   ```bash
   dbt run
   ```

3. **Run dbt Tests**:
   Validate the data quality:
   ```bash
   dbt test
   ```

4. **Generate Documentation**:
   Generate and serve documentation:
   ```bash
   dbt docs generate
   dbt docs serve
   ```
   Access the documentation in your browser at `http://localhost:8080`.

---

## Data Transformations

### 1. **Total Turnover by Venue**
- **Model**: `total_turnover_by_venue.sql`
- **Logic**: Calculates the sum of `turnover_sum` for each `venue_code`.

### 2. **Revenue by EGM and Venue**
- **Model**: `revenue_by_egm_and_venue.sql`
- **Logic**: Aggregates `gmp_sum` by `egm_description` and `venue_code`.

### 3. **Daily Summary**
- **Model**: `daily_summary.sql`
- **Logic**: Creates a summary of daily turnover and revenue metrics for each `bus_date`.

### 4. **Incremental dbt Model
 **Model**: `game_performance_incremental.sql`
The incremental dbt model is designed to handle new and updated records from an incremental data source and integrate them into the primary GAME_PERFORMANCE_DATA table. By using dbt’s incremental materialization, the model ensures efficient updates without reprocessing the entire dataset.

Key Features
## Incremental Logic:

Loads only the new or updated records from the incremental data source into the target table.
Avoids duplicating existing records by comparing the BUS_DATE.
## Full Refresh Capability:

During the first run or a full refresh, the model processes all the incremental data and loads it into the target table.
## Date-Based Incremental Processing:

Uses the BUS_DATE column to identify which rows are new or updated.

---

## Data Quality Checks

### Implemented Tests
1. **Non-null values**:
   - Key columns: `bus_date`, `venue_code`, `egm_description`, `manufacturer`, `fp`.
2. **Valid date formats**:
   - Validates that `bus_date` follows the `YYYY-MM-DD` format.
3. **Positive values**:
   - Ensures that `turnover_sum` and `games_played_sum` are positive.

### Running Tests
Run all tests with the command:
```bash
dbt test
```

### Test Results
View test results in the terminal or as part of the generated documentation.

---

## Notes
- Ensure your PostgreSQL instance is running before executing the pipeline.
- Update the `dbt_project.yml` file if your database or schema name changes.

For questions or issues, feel free to contact the project maintainer.

### README

# Data Pipeline for Game Performance Data

This repository contains a complete data pipeline to ingest, transform, and test game performance data using Snowflake and dbt.

---

## **Pipeline Overview**

### **Steps**

The pipeline.py script orchestrates the end-to-end data pipeline for the Game Performance Data project. It performs the following steps:

## Connect to Snowflake: 
Establishes a connection to your Snowflake account using the snowflake.snowpark library. Ensures secure interaction with the Snowflake database for data ingestion and transformation.

## Load Data into Snowflake: 
Handles both full load and incremental load of game performance data from CSV files into Snowflake tables:

## Full Load:
Reads the CSV file (Data_Engineer_Challenge_input.csv) and loads it into the GAME_PERFORMANCE_DATA table.
## Incremental Load:
Reads the incremental CSV file (incremental_data.csv) and loads it into a staging table GAME_PERFORMANCE_INCREMENTAL.
## Run dbt Models: 
Executes dbt transformations by running models in a predefined sequence:

game_performance_incremental.sql: Merges incremental data into the GAME_PERFORMANCE_DATA table.
daily_summary.sql: Creates daily turnover and revenue summaries.
total_turnover_by_venue.sql: Aggregates total turnover by venue.
revenue_by_egm_and_venue.sql: Aggregates revenue by EGM and venue.
## Run dbt Tests: 
Validates data quality using dbt tests. This ensures:

Non-null values in key columns.
Valid date formats in the BUS_DATE column.
Positive values for TURNOVER_SUM and GAMES_PLAYED_SUM.
## Truncate Staging Table: 
After processing the incremental data, the staging table GAME_PERFORMANCE_INCREMENTAL is truncated to prepare for the next incremental load.

## Pipeline Completion: 
Prints success messages after completing each step to confirm the pipeline execution.
---

## **Setup Instructions**

### **Prerequisites**
- Python 3.9+ installed.
- An active Snowflake account with the required permissions to create tables and schemas.
- dbt installed (version 1.9.0 or compatible).

### **Environment Setup**
1. **Clone the Repository**:
   ```bash
   git clone <repository-url>
   cd <repository-directory>
   ```

2. **Install Dependencies**:
   - Create and activate a virtual environment:
     ```bash
     python -m venv venv
     source venv/bin/activate  # On macOS/Linux
     venv\Scripts\activate     # On Windows
     ```
   - Install the required Python libraries:
     ```bash
     pip install -r requirements.txt
     ```

3. **Configure Snowflake Connection**:
   - Update the `connection_parameters` in `scripts/pipeline.py` with your Snowflake account details.

4. **Configure dbt**:
   - Navigate to the dbt project directory:
     ```bash
     cd my_dbt_project
     ```
   - Edit the `profiles.yml` file (usually located in `~/.dbt/profiles.yml`) to include your Snowflake connection settings. Example:
     ```yaml
     my_dbt_project:
       target: dev
       outputs:
         dev:
           type: snowflake
           account: YM04901.ap-southeast-2
           user: anveshneta
           password: your_password
           role: ACCOUNTADMIN
           warehouse: COMPUTE_WH
           database: ONYX_DB
           schema: GAME_SCHEMA
           threads: 1
     ```

---

## **Running the Pipeline**

1. **Navigate to the Project Root**:
   ```bash
   cd <repository-directory>
   ```

2. **Execute the Pipeline**:
   ```bash
   python scripts/pipeline.py
   ```

3. **Pipeline Output**:
   - **Ingestion**:
     - Loads the data from the CSV file into the `GAME_PERFORMANCE_DATA` table in Snowflake.
   - **Transformation**:
     - Runs dbt models to perform the required transformations.
   - **Testing**:
     - Executes dbt tests to ensure data quality.


## **Data Transformations**

1. **Total Turnover by Venue**:
   - Aggregates the `TURNOVER_SUM` column by venue.

2. **Revenue by EGM and Venue**:
   - Aggregates the `GMP_SUM` column by EGM and venue.

3. **Daily Summary**:
   - Provides a daily summary of turnover and revenue.

---

## **Data Quality Checks**

The pipeline includes the following data quality tests:
1. **Non-Null Checks**:
   - Ensures no null values in key columns: `BUS_DATE`, `VENUE_CODE`, `EGM_DESCRIPTION`, `MANUFACTURER`, `FP`.

2. **Date Format Validation**:
   - Ensures `BUS_DATE` values are in `YYYY-MM-DD` format.

3. **Positive Value Validation**:
   - Ensures `TURNOVER_SUM` and `GAMES_PLAYED_SUM` have positive values.

---

## **Troubleshooting**

1. **Snowflake Connection Issues**:
   - Verify your Snowflake credentials in `pipeline.py` and `profiles.yml`.

2. **dbt Errors**:
   - Ensure the `dbt_project.yml` file is in the correct directory.
   - Run the following to check dbt configuration:
     ```bash
     dbt debug
     ```

3. **CSV Loading Issues**:
   - Ensure the input CSV file is present in the `data/` directory and has valid column headers.

---

## **License**

This project is licensed under the MIT License.

---

## **Contributors**

- Anvesh Neta

