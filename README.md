# Project ETL - Linkedin Jobs Posting 💻

## Overview 
This project simulates a real-world exercise for a Data Engineer role interview. The objectives are:

- Migrate data from CSV files into a relational database (PostgreSQL - AWS RDS).
- Clean and transform data based on defined criteria.
- Visualise metrics using Python libraries to create insightful charts.

## Dataset

The dataset is sourced from [LinkedIn Job Postings on Kaggle](https://www.kaggle.com/datasets/arshkon/linkedin-job-postings) and includes multiple CSV files. Key files used:

- **`job_postings.csv`**:
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

- **`jobs/benefits.csv`**:
  - `job_id`: Links to `job_postings.csv` (BIGINT).
  - `type`: Benefit type (e.g., Pension Scheme; VARCHAR).
  - `inferred`: Whether inferred by LinkedIn (BOOLEAN).

- **`companies/companies.csv`**:
  - `company_id`: Unique company identifier (BIGINT).
  - `name`: Company name (VARCHAR).
  - `company_size`: Size grouping (0-7; INT, nullable).
  - `country`: Headquarters country (VARCHAR).

- **`companies/employee_counts.csv`**:
  - `company_id`: Links to `companies.csv` (BIGINT).
  - `employee_count`: Number of employees (INT).
  - `follower_count`: LinkedIn followers (INT).

---

## Project Structure

| Folder/File                  | Description                                  |
|------------------------------|----------------------------------------------|
| **assets/**                  | Static resources (charts, images, etc.)      |
| **functions/**               | Utility functions                            |
| ├── **db_connection/**       | Database connection module                   |
| │   ├── `connection.py`   | Connects to AWS RDS (Postgres DB) using SQLAlchemy           |
| │   ├── `clean_connection.py`   | Connects to AWS RDS (Postgres DB) using SQLAlchemy - Clean DB          |
| **env/**                     | Environment variables (in `.gitignore`)      |
| ├── `.env`                   | Stores database credentials                  |
| ├── `linkedin_postings_clean_.env`                   | Stores cleaned database credentials                  |
| **notebooks/**               | Jupyter Notebooks for ETL                    |
| ├── `01_raw-data.ipynb`      | Raw data ingestion                           |
| ├── `02_read_data.ipynb` |     Exploratory Data Analysis (EDA)      |
| ├── `03_clean_transform.ipynb` | Data cleaning and transformation                       |
| **pdf/**                     | Project documentation PDFs                   |
| ├── `ETL Project - First delivery.pdf` | Instructions for the project       |
| **pyproject.toml**           | Poetry dependency management file            |
| **README.md**                | This file                                    |

## Tools and Libraries

- **Programming Language:** Python 3.13.1 -> [Download](https://www.python.org/downloads/)
- **Data Handling:** pandas -> [Docs](https://pandas.pydata.org/)
- **Database:** AWS RDS Free Tier (PostgreSQL) -> [Open here](https://aws.amazon.com/rds/free/)
- **Database Interaction:** SQLAlchemy with PyMySQL -> [SQLAlchemy Docs](https://docs.sqlalchemy.org/), [PyMySQL Docs](https://pymysql.readthedocs.io/)
- **Visualisation:** Power BI Desktop -> [Download](https://www.microsoft.com/es-es/power-platform/products/power-bi/desktop)
- **Environment:** Jupyter Notebook -> [VSCode tool used](https://code.visualstudio.com/docs/datascience/jupyter-notebooks)

Dependencies are managed in `pyproject.toml`.

## Installation and Setup

1. **Clone the Repository:**
   ```bash
   git clone https://github.com/SEBASBELMOS/LinkedIn-Jobs-Posting.git
   cd project_etl
   ````

2. **Installing the dependencies with _Poetry_**
    - Windows: 
        - In Powershell, execute this command: 
            ```powershell
            (Invoke-WebRequest -Uri https://install.python-poetry.org -UseBasicParsing).Content | py -
            ```
            <img src="https://github.com/SEBASBELMOS/LinkedIn-Jobs-Posting/blob/main/assets/poetry_installation.png" width="600"/>
        - Press Win + R, type _sysdm.cpl_, and press **Enter**. 
        - Go to the _Advanced_ tab, select _environment variable_.
        - Under System variables, select Path → Click Edit.
        - Click _Edit_ and set the path provided during the installation in **PATH** so that the `poetry` command works. ("C:\Users\username\AppData\Roaming\Python\Scripts")
        - Restart Powershell and execute _poetry --version_.

        
    - Linux
        - In a terminal, execute this command:
            ```bash
            curl -sSL https://install.python-poetry.org | python3 -
            ```
            <img src="https://github.com/SEBASBELMOS/LinkedIn-Jobs-Posting/blob/main/assets/poetry_linux.png" width="600"/>
        -  Now, execute:
            ```bash
            export PATH = "/home/user/.locar/bin:$PATH"
            ```
        -Finally, restart the terminal and execute _poetry --version_.


        <img src="https://github.com/SEBASBELMOS/LinkedIn-Jobs-Posting/blob/main/assets/poetry_linux_installed.png" width="400"/>

3. **Poetry Shell**
    - Enter the Poetry shell in VSCode with _poetry shell_.
    - Then, execute _poetry init_, it will create a file called _pyproject.toml_
    - To add all the dependencies, execute this: 
        ```bash
        poetry add pandas matplotlib psycopg2-binary sqlalchemy python-dotenv seaborn ipykernel dotenv
        ```
    - Install the dependencies with: 
        ```bash
        poetry install
        ```
        In case of error with the .lock file, just execute _poetry lock_ to fix it.
    - Create the kernel with this command (You must choose this kernel when running the notebooks):
        ```bash
        poetry run python -m ipykernel install --user --name project_etl --display-name "linkedin_postings_etl"
        ```

4.  **Virtual Environment (This must be done in Ubuntu or WSL in case you do not want to use Poetry)**
    - Create virtual environment.
        ```bash
        python3 -m venv venv
        ```

    - Activate it using this command:
        ```bash
        source venv/bin/activate 
        ```

    - Install all the requirements and libraries with this command:
        ```bash
        pip install -r requirements.txt 
        ```


5. **AWS RDS Free Tier / Supabase**
    We decided to use AWS RDS instead of Supabase because this was an amazing skill to add to our portfolios and it provides wider options when it comes to storage and availability.
    
    Amazon RDS (Relational Database Service) hosts our database, running PostgreSQL 16.3, with a size of approximately 449 MB (originally 414 MB locally). Follow these steps to create and connect to it.
    
    1. Go to [AWS Management Console](https://aws.amazon.com/console/) and sign in with your AWS account credentials.
    2. When logged in, click "Services" > "RDS" under the "Database" section.
    3. Create a new instance
        - **Engine**: Select "PostgreSQL".
        - **Version**: Choose "16.3" (or the closest available version).
        - **DB Instance Size**: Select "db.t3.micro" (1 vCPU, 1 GB RAM).
        - **DB Instance Identifier**: Enter `database_name`.
        - **Master Username**: `postgres` (or any username you want).
        - **Master Password**: `password` (or a secure password, ensuring you update `.env` accordingly).
        - **Public Accessibility**: Set to "Yes" to allow external connections.
        - **VPC Security Group**: Ensure it allows inbound traffic on port `5432` (TCP) from `0.0.0.0/0` for public access.
        - Click "Create database" and wait for the instance to launch (~5-10 minutes).
    4. After updating the `.env` file, execute the notebook #1.
        Host: Instance Endpoint.
        Port: 5432.
        Database: database_name.
        Username: postgres (or any other user created).
        Password: Set during project creation (e.g., password).

    Supabase provides a managed PostgreSQL database, and this is how you create and connect to it.

    1. Go to Supabase.com and sign up or log in.
    2. Create a new project, set a password and choose a region close to you.
    3. Open _SQL Editor_, and create a database with:
        ```sql
        CREATE DATABASE "database_name";
        ```
    4. After updating the `.env` file with the credentials, execute the notebook #1.
        Host: <project-ref>.supabase.co.
        Port: 5432.
        Database: database_name.
        Username: postgres (or any other user created).
        Password: Set during project creation (e.g., password).

6. **Database Google Cloud Platform in case you do not want to use Supabase or AWS RDS**
    > To create the databases in GCP, you can follow this [guide](https://github.com/SEBASBELMOS/LinkedIn-Jobs-Posting/blob/main/docs/guides/google_cloud_config.md)

    - Use the `public IP` for connections, and ensure the IP `0.0.0.0/0` is added to authorised networks for testing.

7. **Enviromental variables**
    >Realise this in VS Code.

    To establish a connection with the database, we use a module called _connection.py_ and _clean\_connection_. These Python scripts retrieves a file containing our environment variables. Here’s how to create it:
    1. Inside the cloned repository, create a new directory named *env/*.
    2. Within that directory, create a file called *.env*.
    3. In the *.env file*, define the following six environment variables (without double quotes around values):
        ```python
        PG_HOST = #host address, e.g. localhost or 127.0.0.1
        PG_PORT = #PostgreSQL port, e.g. 5432

        PG_USER = #your PostgreSQL user
        PG_PASSWORD = #your user password
        
        PG_DRIVER = postgresql+psycopg2
        PG_DATABASE = #your database name, e.g. postgres
        ```


---

## Workflow

<img src="https://github.com/SEBASBELMOS/LinkedIn-Jobs-Posting/blob/main/assets/etl_pipeline.png" width="600"/>

### Notebooks

1. **Ingest Raw Data (notebooks\01_raw-data.ipynb):**
    > 🚧 Note: Run this notebook only once, as it migrates all the data to your database.
    - Open `notebooks/01_raw-data.ipynb`in VS Code.
    - Select the Python kernel.
    - Run to download the dataset and load it into PostgreSQL.

2. ** Read data and Exploratory Data Analysis (notebooks\02_read_data.ipynb):**
    - Open `notebooks/02_read_data.ipynb`.
    - Explore the Data:
        - Review the structure, data types, and sample entries.
        - Identify and document missing values and inconsistencies.
        - Generate summary statistics and visualise distributions.
        - Use the insights from this analysis to inform the cleaning and transformation steps in subsequent notebooks.

3. **Clean and Transform (notebooks\03_clean_transform.ipynb):**
    - Open `notebooks/03_clean_transform.ipynb`.
    - Perform data cleaning, handle missing values, and transform timestamps/salaries.
    - Load clean data into a new DB in PostgreSQL.

---

## Airflow Pipeline

Before launching Apache Airflow, you need to export the `AIRFLOW_HOME` environment variable. This variable establishes the project directory in which you will be working with Airflow.

```bash
export AIRFLOW_HOME="$(pwd)/airflow"
```

Run Apache Airflow with this command:

```bash
airflow standalone
```
Ensure that Apache Airflow can access the modules in the `src` directory by specifying the absolute path to that folder in the `plugins_folder` setting within the `airflow.cfg` file. You might need to restart Apache Airflow if you encounter any DAG import errors.

    <img src="https://github.com/SEBASBELMOS/LinkedIn-Jobs-Posting/blob/main/assets/plugins_folder_airflow.png" width="400"/>

> [!IMPORTANT]
> Open [http://localhost:8080](http://localhost:8080/) in order to open the Airflow GUI and ran the DAG.

    <img src="https://github.com/SEBASBELMOS/LinkedIn-Jobs-Posting/blob/main/assets/airflow_gui.png" width="400"/>

---

## Power BI Connection (Not necessary)
1. Open Power BI Desktop and create a new dashboard. 
2. Select the _Get data_ option, then choose the "_PostgreSQL Database_" option.

    <img src="https://github.com/SEBASBELMOS/LinkedIn-Jobs-Posting/blob/main/assets/pbi.png" width="400"/>

3. Insert the _PostgreSQL Server_ and _Database Name_.
    
    <img src="https://github.com/SEBASBELMOS/LinkedIn-Jobs-Posting/blob/main/assets/postgres_pbi.png" width="400"/>

4. Fill the following fields with your Postgres credentials.
    
    <img src="https://github.com/SEBASBELMOS/LinkedIn-Jobs-Posting/blob/main/assets/postrgres_access_pbi.png" width="400"/>    

5. After establishing the connection, these tables will be displayed.You need to select the "_candidates_hired_" table, and then you can start creating your own dashboards.
    
    <img src="https://github.com/SEBASBELMOS/LinkedIn-Jobs-Posting/blob/main/assets/tables.png" width="200"/>

- Open the Power BI Visualisation [here](https://app.powerbi.com/view?r=eyJrIjoiMzk3ZTA5YzEtYjkwNS00N2FjLWE5NDEtMmMzOWZkZTEzN2RkIiwidCI6IjY5M2NiZWEwLTRlZjktNDI1NC04OTc3LTc2ZTA1Y2I1ZjU1NiIsImMiOjR9)

---

## **Authors**  
Created by:

**Sebastian Belalcazar**. [LinkedIn](https://www.linkedin.com/in/sebasbelmos/) / [GitHub](https://github.com/SEBASBELMOS)

**Gabriel Edwards**. [LinkedIn](https://www.linkedin.com/in/gabriel-martinez-a12068267/) / [GitHub](https://github.com/XGabrielEdwardsX)

Connect with us for feedback, suggestions, or collaboration opportunities!

---
