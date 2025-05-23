# Project ETL - LinkedIn & USAJOBS Job Postings 💻

## Overview 
This project simulates a real-world exercise for a Data Engineer role interview. The objectives are:

- Extract data from CSV files (LinkedIn) and an API (USAJOBS), and migrate it into a relational database (PostgreSQL - AWS RDS).
- Clean, transform, and combine data from both sources based on defined criteria.
- Visualize metrics using Python libraries to create insightful charts and analyze trends in job postings.
- Implement a real-time data pipeline using Apache Kafka to stream and analyze job posting data, with a live dashboard.

## Dataset

### **LinkedIn Job Postings**
The LinkedIn dataset is sourced from [LinkedIn Job Postings on Kaggle](https://www.kaggle.com/datasets/arshkon/linkedin-job-postings) and includes multiple CSV files. Key files used:

- **`data/postings.csv`**:
  - `job_id`: Unique job identifier (BIGINT).
  - `company_id`: Links to `companies.csv` (BIGINT).
  - `title`: Job title (VARCHAR).
  - `max_salary`, `min_salary`, `med_salary`: Salary details (FLOAT, nullable).
  - `pay_period`: Salary period (e.g., Hourly, Monthly; VARCHAR).
  - `formatted_work_type`: Work type (e.g., Full-time; VARCHAR).
  - `location`: Job location (VARCHAR).
  - `remote_allowed`: Remote work permitted (BOOLEAN, nullable).
  - `views`: Number of views (INT, nullable).
  - `applies`: Number of applications (INT, nullable).
  - `formatted_experience_level`: Experience level (e.g., Entry; VARCHAR, nullable).
  - `listed_time`, `expiry`: Unix timestamps (BIGINT).

- **`data/jobs/benefits.csv`**:
  - `job_id`: Links to `postings.csv` (BIGINT).
  - `type`: Benefit type (e.g., Pension Scheme; VARCHAR).
  - `inferred`: Whether inferred by LinkedIn (BOOLEAN).

- **`data/companies/companies.csv`**:
  - `company_id`: Unique company identifier (BIGINT).
  - `name`: Company name (VARCHAR).
  - `company_size`: Size grouping (0-7; INT, nullable).
  - `country`: Headquarters country (VARCHAR).

- **`data/companies/employee_counts.csv`**:
  - `company_id`: Links to `companies.csv` (BIGINT).
  - `employee_count`: Number of employees (INT).
  - `follower_count`: LinkedIn followers (INT).

### **USAJOBS API**
The USAJOBS dataset is sourced via the [USAJOBS API](https://developer.usajobs.gov/). The API provides job postings from the U.S. federal government. Key fields extracted include:

- `PositionID`: Unique job identifier (VARCHAR).
- `PositionTitle`: Job title (VARCHAR).
- `Location`, `City`, `State`, `Country`: Job location details (VARCHAR).
- `Latitude`, `Longitude`: Geographic coordinates (FLOAT, nullable).
- `Organization`, `Department`: Organization and department names (VARCHAR).
- `MinSalary`, `MaxSalary`, `SalaryInterval`: Salary details (FLOAT, VARCHAR).
- `JobCategory`, `JobGrade`: Job category and grade (VARCHAR).
- `Schedule`, `OfferingType`: Work schedule and offering type (e.g., Full-time, Permanent; VARCHAR).
- `StartDate`, `EndDate`, `PublicationDate`, `CloseDate`: Key dates (TIMESTAMP, nullable).
- `TeleworkEligible`: Remote work eligibility (BOOLEAN).
- `SecurityClearance`, `PromotionPotential`, `TravelCode`: Additional job details (VARCHAR).
- `HiringPath`, `TotalOpenings`: Hiring path and number of openings (VARCHAR).

**Total Combined Records**: After merging and deduplicating, the combined dataset from LinkedIn and USAJOBS contains approximately 131,930 job postings.

---

## Project Structure

| Folder/File                  | Description                                  |
|------------------------------|----------------------------------------------|
| **assets/**                  | Static resources (charts, images, etc.)      |
| **airflow/**                 | Apache Airflow Resources and Configuration files     |
| ├── **dags/**                | Stores Apache Airflow Dags (e.g., `etl_process.py`) |
| ├── **dags/task/**           | Stores Airflow Tasks (e.g., `extract.py`, `transform_api.py`) |
| **data/**                    | Raw LinkedIn CSV files                       |
| ├── **companies/**           | Company-related CSV files                    |
| ├── **jobs/**                | Job-related CSV files                        |
| ├── **mappings/**            | Mapping files for industries and skills      |
| ├── **postings.csv**         | Main job postings file                       |
| **data_api/**                | USAJOBS API data (e.g., `usajobs_data.csv`)  |
| **data_merged/**             | Merged datasets (e.g., `merge.csv`, `merge_with_api.csv`) |
| **docs/**                    | Documentation, Guides, and workshop PDFs     |
| ├── **guides/**              | Guides (e.g., `google_cloud_config.md`)      |
| ├── **pdf/**                 | PDFs (e.g., `ETL Project - First delivery.pdf`) |
| **env/**                     | Environment configuration files              |
| **kafka/**                   | Kafka components for real-time processing    |
| ├── **consumer/**            | Kafka consumer code and config (e.g., `consumer.py`) |
| ├── **dash_app/**            | Dash dashboard code and assets               |
| ├── **data_json/**           | JSON output (e.g., `current_count.json`)     |
| ├── **producer/**            | Kafka producer code and config (e.g., `producer.py`) |
| ├── **docker-compose.yml**   | Docker Compose configuration for Kafka       |
| **notebooks/**               | Jupyter Notebooks for ETL                    |
| ├── `00_download_raw_data.ipynb` | Raw data download                         |
| ├── `01_eda_and_load_raw.ipynb` | EDA and load for LinkedIn data            |
| ├── `02_transform_db.ipynb`   | Transform LinkedIn data to database        |
| ├── `03_clean-transform.ipynb`| Clean and transform combined data          |
| ├── `04_api.ipynb`           | API data extraction and EDA (USAJOBS)       |
| ├── `05_merge-data.ipynb`    | Merge LinkedIn and USAJOBS data             |
| ├── `dimensional_model.ipynb`| Dimensional model creation                  |
| ├── `final.ipynb`            | Final analysis and visualizations           |
| ├── `transform_csv.ipynb`    | CSV transformation scripts                  |
| **README.md**                | This file                                    |
| **settings.py**              | Project settings                             |
| **standalone_admin_password.txt** | Airflow admin password                    |
| **webserver_config.py**      | Airflow webserver configuration              |
| **airflow.cfg**              | Airflow configuration file                   |
| **airflow.db**               | Airflow metadata database                    |
| **airflow-webserver.pid**    | Airflow webserver PID file                   |

## Tools and Libraries

- **Programming Language:** Python 3.13.1 -> [Download](https://www.python.org/downloads/)
- **Data Handling:** pandas -> [Docs](https://pandas.pydata.org/)
- **Database:** Google Cloud Platform (PostgreSQL) -> [Open here](https://cloud.google.com/sql/docs/postgres)
- **Database Interaction:** SQLAlchemy with PyMySQL -> [SQLAlchemy Docs](https://docs.sqlalchemy.org/), [PyMySQL Docs](https://pymysql.readthedocs.io/)
- **API Interaction:** requests, urllib3 -> [requests Docs](https://docs.python-requests.org/)
- **Visualization:** Power BI Desktop -> [Download](https://www.microsoft.com/es-es/power-platform/products/power-bi/desktop), Seaborn, Matplotlib -> [Seaborn Docs](https://seaborn.pydata.org/), [Matplotlib Docs](https://matplotlib.org/), Plotly -> [Plotly Docs](https://plotly.com/python/)
- **Workflow Management:** Apache Airflow -> [Docs](https://airflow.apache.org/)
- **Real-Time Processing:** Apache Kafka -> [Docs](https://kafka.apache.org/documentation/), kafka-python
- **Dashboard:** Dash, dash-bootstrap-components -> [Dash Docs](https://dash.plotly.com/)
- **Environment:** Jupyter Notebook -> [VSCode tool used](https://code.visualstudio.com/docs/datascience/jupyter-notebooks), Docker -> [Docs](https://docs.docker.com/)
- **Dependencies:** Managed in `requirements.txt` and Kafka-specific `requirements.txt` files

## Installation and Setup

1. **Clone the Repository:**
   ```bash
   git clone https://github.com/SEBASBELMOS/LinkedIn-Jobs-Posting.git
   cd LinkedIn-Jobs-Posting
   ```

2. **Virtual Environment (This must be done in Ubuntu or WSL)**
    - Create virtual environment:
        ```bash
        python3 -m venv venv
        ```
    - Activate it:
        ```bash
        source venv/bin/activate 
        ```
    - Install requirements:
        ```bash
        pip install -r requirements.txt 
        ```

3. **AWS RDS Free Tier / Supabase**
    We decided to use AWS RDS instead of Supabase because this was an amazing skill to add to our portfolios and it provides wider options when it comes to storage and availability.
    
    Amazon RDS (Relational Database Service) hosts our database, running PostgreSQL 16.3, with a size of approximately 449 MB (originally 414 MB locally). Follow these steps to create and connect to it:
    
    1. Go to [AWS Management Console](https://aws.amazon.com/console/) and sign in with your AWS account credentials.
    2. Click "Services" > "RDS" under the "Database" section.
    3. Create a new instance:
        - **Engine**: Select "PostgreSQL".
        - **Version**: Choose "16.3" (or the closest available version).
        - **DB Instance Size**: Select "db.t3.micro" (1 vCPU, 1 GB RAM).
        - **DB Instance Identifier**: Enter `database_name`.
        - **Master Username**: `postgres` (or any username you want).
        - **Master Password**: `password` (or a secure password, ensuring you update `.env` accordingly).
        - **Public Accessibility**: Set to "Yes" to allow external connections.
        - **VPC Security Group**: Ensure it allows inbound traffic on port `5432` (TCP) from `0.0.0.0/0` for public access.
        - Click "Create database" and wait for the instance to launch (~5-10 minutes).
    4. After updating the `.env` file, execute notebook #1:
        - Host: Instance Endpoint.
        - Port: 5432.
        - Database: database_name.
        - Username: postgres (or any other user created).
        - Password: Set during project creation (e.g., password).

    Alternatively, Supabase provides a managed PostgreSQL database. Steps to create and connect:
    
    1. Go to Supabase.com and sign up or log in.
    2. Create a new project, set a password, and choose a region close to you.
    3. Open _SQL Editor_, and create a database with:
        ```sql
        CREATE DATABASE "database_name";
        ```
    4. After updating the `.env` file with the credentials, execute notebook #1:
        - Host: <project-ref>.supabase.co.
        - Port: 5432.
        - Database: database_name.
        - Username: postgres (or any other user created).
        - Password: Set during project creation (e.g., password).

4. **Database Google Cloud Platform (Alternative to AWS RDS or Supabase)**
    > To create the databases in GCP, follow this [guide](https://github.com/SEBASBELMOS/LinkedIn-Jobs-Posting/blob/main/docs/guides/google_cloud_config.md).
    - Use the `public IP` for connections, and ensure the IP `0.0.0.0/0` is added to authorized networks for testing.

5. **Kafka Setup**
    - Ensure Docker and Docker Compose are installed.
    - Navigate to the `kafka/` directory and run:
        ```bash
        docker-compose up --build
        ```
    - This will start Kafka, Zookeeper, the producer, consumer, and dashboard.
    - Access the dashboard at [http://localhost:8050](http://localhost:8050).

---

## Workflow

<img src="https://github.com/SEBASBELMOS/LinkedIn-Jobs-Posting/blob/main/assets/etl_pipeline.png" width="600"/>

## Dimensional Model

<img src="https://github.com/SEBASBELMOS/workshop-002/blob/main/assets/dim_model.png" width="500"/>

---

## Notebooks

1. **Download Raw Data (notebooks/00_download_raw_data.ipynb):**
    > 🚧 Note: Run this notebook only once to download LinkedIn data.
    - Open `notebooks/00_download_raw_data.ipynb` in VS Code.
    - Select the Python kernel.
    - Run to download the LinkedIn dataset.

2. **EDA and Load Raw Data (notebooks/01_eda_and_load_raw.ipynb):**
    - Open `notebooks/01_eda_and_load_raw.ipynb`.
    - Explore LinkedIn data: structure, data types, missing values.
    - Load raw data into PostgreSQL.

3. **Transform Database (notebooks/02_transform_db.ipynb):**
    - Open `notebooks/02_transform_db.ipynb`.
    - Transform LinkedIn data and load into a structured schema.

4. **Clean and Transform Combined Data (notebooks/03_clean-transform.ipynb):**
    - Open `notebooks/03_clean-transform.ipynb`.
    - Combine and clean LinkedIn and USAJOBS data (~131,930 records).
    - Standardize columns and handle missing values.

5. **API Data Extraction and EDA (notebooks/04_api.ipynb):**
    - Open `notebooks/04_api.ipynb`.
    - Extract USAJOBS API data and save to `data_api/usajobs_data.csv`.
    - Perform EDA and normalize salaries.

6. **Merge Data (notebooks/05_merge-data.ipynb):**
    - Open `notebooks/05_merge-data.ipynb`.
    - Merge LinkedIn and USAJOBS data into `data_merged/merge_with_api.csv`.
    - Validate the combined dataset.

7. **Dimensional Model (notebooks/dimensional_model.ipynb):**
    - Open `notebooks/dimensional_model.ipynb`.
    - Create and populate the dimensional model in PostgreSQL.

8. **Final Analysis (notebooks/final.ipynb):**
    - Open `notebooks/final.ipynb`.
    - Generate final visualizations and insights.

9. **Transform CSV (notebooks/transform_csv.ipynb):**
    - Open `notebooks/transform_csv.ipynb`.
    - Handle CSV-specific transformations for interim data.

---

## Visualizations

The project includes visualizations to analyze job posting trends, generated in the notebooks and summarized in `docs/pdf/ETL Project - First delivery.pdf`:

- **Job Postings Over Time**: Shows a peak of 93,000 job postings in 2025.
- **Job Counts by Work Type**: Highlights distribution of work types, with many "unknown" entries.
- **Remote vs. On-Site Job Postings**: Analyzes remote versus on-site job proportions.
- **Salary Average by Experience Level**: Displays average salaries (~90K-91.9K USD) with little variation.
- **Salary Distribution**: Visualizes normalized salary distributions.

### **Real-Time Dashboard**
- A Dash-based dashboard (`kafka/dash_app/dashboard.py`) provides live insights from Kafka streams:
  - **Live Job Count**: Number of active job offers in the last 60 seconds.
  - **Top 5 Industries**: Bar chart of industries with the most postings.
  - **Top 5 States**: Bar chart of states with the most postings.
  - Access at [http://localhost:8050](http://localhost:8050) after running `docker-compose up` in the `kafka/` directory.

---

## Airflow Pipeline

Before launching Apache Airflow, export the `AIRFLOW_HOME` environment variable:

```bash
export AIRFLOW_HOME="$(pwd)/airflow"
```

Run Apache Airflow with:

```bash
airflow standalone
```

Ensure Apache Airflow can access the `src` directory by updating the `plugins_folder` setting in `airflow.cfg`. Restart Airflow if you encounter DAG import errors.

<img src="https://github.com/SEBASBELMOS/LinkedIn-Jobs-Posting/blob/main/assets/plugins_folder_airflow.png" width="400"/>

> [!IMPORTANT]
> Open [http://localhost:8080](http://localhost:8080/) to access the Airflow GUI and run the DAG.

<img src="https://github.com/SEBASBELMOS/LinkedIn-Jobs-Posting/blob/main/assets/airflow_gui.png" width="400"/>

---

## Power BI Connection

1. Open Power BI Desktop and create a new dashboard.
2. Select the _Get Data_ option, then choose "_PostgreSQL Database_".

    <img src="https://github.com/SEBASBELMOS/LinkedIn-Jobs-Posting/blob/main/assets/pbi.png" width="400"/>

3. Insert the _PostgreSQL Server_ and _Database Name_.

    <img src="https://github.com/SEBASBELMOS/LinkedIn-Jobs-Posting/blob/main/assets/postgres_pbi.png" width="400"/>

4. Fill in the fields with your PostgreSQL credentials.

    <img src="https://github.com/SEBASBELMOS/LinkedIn-Jobs-Posting/blob/main/assets/postrgres_access_pbi.png" width="400"/>

5. After establishing the connection, select the "_candidates_hired_" table to start creating dashboards.

    <img src="https://github.com/SEBASBELMOS/LinkedIn-Jobs-Posting/blob/main/assets/tables.png" width="200"/>

- Open the Power BI Visualization [here](https://app.powerbi.com/view?r=eyJrIjoiYzVmMGFjYTktNzE2Ni00MWNhLWE2ODktOWMwZTY2OTdiMGU5IiwidCI6IjY5M2NiZWEwLTRlZjktNDI1NC04OTc3LTc2ZTA1Y2I1ZjU1NiIsImMiOjR9).

---

## Authors

Created by:

**Sebastian Belalcazar**  
[LinkedIn](https://www.linkedin.com/in/sebasbelmos/) | [GitHub](https://github.com/SEBASBELMOS)

**Gabriel Edwards**  
[LinkedIn](https://www.linkedin.com/in/gabriel-martinez-a12068267/) | [GitHub](https://github.com/XGabrielEdwardsX)

Connect with us for feedback, suggestions, or collaboration opportunities!