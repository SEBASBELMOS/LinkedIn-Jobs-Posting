# Project ETL - Linkedin Jobs Posting 💻

## Overview 
This project simulates a real-world exercise for a Data Engineer role interview. The objectives are:

- Migrate data from CSV files into a relational database (MySQL).
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

- **`job_details/benefits.csv`**:
  - `job_id`: Links to `job_postings.csv` (BIGINT).
  - `type`: Benefit type (e.g., Pension Scheme; VARCHAR).
  - `inferred`: Whether inferred by LinkedIn (BOOLEAN).

- **`company_details/companies.csv`**:
  - `company_id`: Unique company identifier (BIGINT).
  - `name`: Company name (VARCHAR).
  - `company_size`: Size grouping (0-7; INT, nullable).
  - `country`: Headquarters country (VARCHAR).

- **`company_details/employee_counts.csv`**:
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
| │   ├── `db_connection.py`   | Connects to MySQL using SQLAlchemy           |
| **env/**                     | Environment variables (in `.gitignore`)      |
| ├── `.env`                   | Stores database credentials                  |
| **notebooks/**               | Jupyter Notebooks for ETL                    |
| ├── `01_raw-data.ipynb`      | Raw data ingestion                           |
| ├── `02_clean_transform.ipynb` | Data cleaning and transformation          |
| ├── `03_visualisation.ipynb` | Metrics visualisation                       |
| **pdf/**                     | Project documentation PDFs                   |
| ├── `ETL Project - First delivery.pdf` | Instructions for the project               |
| **pyproject.toml**           | Poetry dependency management file            |
| **README.md**                | This file                                    |

## Tools and Libraries

- **Programming Language:** Python 3.13.1 -> [Download](https://www.python.org/downloads/)
- **Data Handling:** pandas -> [Docs](https://pandas.pydata.org/)
- **Database:** MySQL -> [Download](https://dev.mysql.com/downloads/installer/)
- **Database Interaction:** SQLAlchemy with PyMySQL -> [SQLAlchemy Docs](https://docs.sqlalchemy.org/), [PyMySQL Docs](https://pymysql.readthedocs.io/)
- **Visualisation:** Power BI Desktop -> [Download](https://www.microsoft.com/es-es/power-platform/products/power-bi/desktop)
- **Environment:** Jupyter Notebook -> [VSCode tool used](https://code.visualstudio.com/docs/datascience/jupyter-notebooks)

Dependencies are managed in `pyproject.toml`.

## Installation and Setup

1. **Clone the Repository:**
   ```bash
   git clone hhttps://github.com/SEBASBELMOS/project_etl.git
   cd project_etl
   ````

2. **Installing the dependencies with _Poetry_**
    - Windows: 
        - In Powershell, execute this command: 
            ```powershell
            (Invoke-WebRequest -Uri https://install.python-poetry.org -UseBasicParsing).Content | py -
            ```
            <img src="https://github.com/SEBASBELMOS/project_etl/blob/main/assets/poetry_installation.png" width="600"/>
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
            <img src="https://github.com/SEBASBELMOS/project_etl/blob/main/assets/poetry_linux.png" width="600"/>
        -  Now, execute:
            ```bash
            export PATH = "/home/user/.locar/bin:$PATH"
            ```
        -Finally, restart the terminal and execute _poetry --version_.


        <img src="https://github.com/SEBASBELMOS/project_etl/blob/main/assets/poetry_linux_installed.png" width="400"/>

3. **Poetry Shell**
    - Enter the Poetry shell with _poetry shell_.
    - Then, execute _poetry init_, it will create a file called _pyproject.toml_
    - To add all the dependencies, execute this: 
        ```bash
        poetry add pandas matplotlib mysql-connector-python sqlalchemy python-dotenv seaborn ipykernel dotenv kagglehub
        ```
    - Install the dependencies with: 
        ```bash
        poetry install
        ```
        In case of error with the .lock file, just execute _poetry lock_ to fix it.
    - Create the kernel with this command (You must choose this kernel when running the notebooks):
        ```bash
        poetry run python -m ipykernel install --user --name project_etl --display-name "Python (project_etl)"
        ```

4. **MySQL Database**
    - Install MySQL with this [link here](https://dev.mysql.com/downloads/installer/)
    - Open a terminal and execute this command, If the user has a password, you will be prompted to enter it: 
        ```bash
        mysql -u username -p
        ```
    - Create a new database with this command:
        ```bash 
        CREATE DATABASE database_name;
        ```
    - This is the information you need to add to the _.env_ file in the next step.

5. **Enviromental variables**
    >Realise this in VS Code.

    To establish a connection with the database, we use a module called _connection.py_. This Python script retrieves a file containing our environment variables. Here’s how to create it:
    1. Inside the cloned repository, create a new directory named *env/*.
    2. Within that directory, create a file called *.env*.
    3. In the *.env file*, define the following six environment variables (without double quotes around values):
        ```bash
        PG_HOST = #host address, e.g. localhost or 127.0.0.1
        PG_PORT = #MySQL port, e.g. 3306

        PG_USER = #your MySQL user
        PG_PASSWORD = #your user password
        
        PG_DRIVER = mysql+mysqlconnector
        PG_DATABASE = #your database name, e.g. mysql
        ```

---

## Running the Project

1. **Ingest Raw Data:**
    - Open `notebooks/01_raw-data.ipynb`in VS Code.
    - Select the Python (*project_etl*) kernel.
    - Run to download the dataset and load it into MySQL.

2. **Clean and Transform:**
    - Open `notebooks/02_clean_transform.ipynb`.
    - Clean missing values and transform timestamps/salaries.

3. **Visualise Metrics:**
    - Open `notebooks/03_visualisation.ipynb`.
    - Generate charts.

---


---

## **Authors**  
Created by:

**Sebastian Belalcazar Mosquera**. [LinkedIn](https://www.linkedin.com/in/sebasbelmos/) / [GitHub](https://github.com/SEBASBELMOS)

**Gabriel Edwards**. [LinkedIn](https://www.linkedin.com/in/gabriel-martinez-a12068267/) / [GitHub](https://github.com/XGabrielEdwardsX)

**Dillian Madroñero**. [LinkedIn]() / [GitHub]()

Connect with us for feedback, suggestions, or collaboration opportunities!

---
