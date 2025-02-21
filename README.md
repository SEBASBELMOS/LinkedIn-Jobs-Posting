# Project ETL - Linkedin Jobs Posting 💻

## Overview 



## Dataset Information

The CSV contains .

- First Column ➜ Format


## Project Structure

| Folder/File            | Description |
|------------------------|------------|
| **assets**             | Static resources (images, documentation, etc.) |
| **functions/**        | Utility functions |
| ├── db_connection/    | Database connection module |
| │ ├── db_connection.py | Connects to PostgreSQL using SQLAlchemy |
| **env/**              | Environment variables (ignored in .gitignore) |
| ├── .env             | tores database credentials |
| **notebooks/**        | Jupyter Notebooks with analysis |
├── 01_raw-data.ipynb    | Raw data ingestion |
| **pdf/**              | Documentation and workshop PDFs |
| ├── ETL Project - First delivery.pdf     | PDF with instructions for the workshop |
| **pyproject.toml**    | Poetry dependency management file |
| **README.md**         | This file |

## Tools and Libraries

- **Programming Language:** Python 3.13.1 -> [Download here](https://www.python.org/downloads/)
- **Data Handling:** pandas -> [Documentation here](https://pandas.pydata.org/)
- **Database:** PostgreSQL -> [Download here](https://www.postgresql.org/download/)
- **Database Interaction:** SQLAlchemy -> [Documentation here](https://docs.sqlalchemy.org/)
- **Visualisation:** Power BI Desktop -> [Download here](https://www.microsoft.com/es-es/power-platform/products/power-bi/desktop)
- **Environment:** Jupyter Notebook -> [VSCode tool used](https://code.visualstudio.com/docs/datascience/jupyter-notebooks)

All the libraries are included in the Poetry project config file (_pyproject.toml_).

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
            <img src="https://github.com/SEBASBELMOS/workshop-001/blob/main/assets/poetry_installation.png" width="600"/>
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
            <img src="https://github.com/SEBASBELMOS/workshop-001/blob/main/assets/poetry_linux.png" width="600"/>
        -  Now, execute:
            ```bash
            export PATH = "/home/user/.locar/bin:$PATH"
            ```
        -Finally, restart the terminal and execute _poetry --version_.


        <img src="https://github.com/SEBASBELMOS/workshop-001/blob/main/assets/poetry_linux_installed.png" width="400"/>

3. **Poetry Shell**
    - Enter the Poetry shell with _poetry shell_.
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
        poetry run python -m ipykernel install --user --name workshop-001 --display-name "Python (workshop-001)"
        ```

4. **PostgreSQL Database**
    - Install PostgreSQL with this [link here](https://www.postgresql.org/download/)
    - Open a terminal and execute this command, If the **postgres** user has a password, you will be prompted to enter it: 
        ```bash
        psql -U postgres
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
        ```python
        PG_HOST = #host address, e.g. localhost or 127.0.0.1
        PG_PORT = #PostgreSQL port, e.g. 5432

        PG_USER = #your PostgreSQL user
        PG_PASSWORD = #your user password
        
        PG_DRIVER = postgresql+psycopg2
        PG_DATABASE = #your database name, e.g. postgres
        ```

---