# How to create a PostgreSQL instance in Google Cloud <img src="https://yt3.googleusercontent.com/ytc/AIdro_n94STjDykDksYxhfE4RhM1BT4R2H69tIAHav8jrey03qQ=s900-c-k-c0x00ffffff-no-rj" alt="GCP" width="25px"/>

1. **Create an account in Google Cloud**
    - Log in or create a new account in Google Cloud, then create a project.

        <img src="https://github.com/SEBASBELMOS/workshop-002/blob/main/assets/account_gcp.png" width="600"/>

2. **Create a new instance using the Cloud SQL Console**
    - Open the navigation bar, then navigate to _SQL_ (You can also use the search bar and type _SQL_).

        <img src="https://github.com/SEBASBELMOS/workshop-002/blob/main/assets/gcp_navigation.png" width="50"/>

    - Click on _Create instance_.

        <img src="https://github.com/SEBASBELMOS/workshop-002/blob/main/assets/sql_gcp.png" width="600"/>
    
    - Select PostgreSQL.

        <img src="https://github.com/SEBASBELMOS/workshop-002/blob/main/assets/db_choice_gcp.png" width="600"/>

3. **Instance Configuration**
    - In Cloud SQL edition, I suggest you to use _Enterprise_ plan to reduce costs and following your requirements, choose either _Sandbox_ or _Development_ preset.

        <img src="https://github.com/SEBASBELMOS/workshop-002/blob/main/assets/cloud_db_config.png" width="600"/>

    - Select an ID for the instance, Username and Password for the database (_the default user is `**postgres**`_).

    - Select the PostgreSQL version you want to use.

    - Configure the region and zone where you require your instance to be hosted.

        <img src="https://github.com/SEBASBELMOS/workshop-002/blob/main/assets/region_gcp.png" width="300"/>

    - Configure the machine type, specifications (memory, CPU) as you require.

        <img src="https://github.com/SEBASBELMOS/workshop-002/blob/main/assets/machine_config_gcp.png" width="300"/>

    - You must add the IP `0.0.0.0/0` to the list of authorised networks in _Connections_.

        <img src="https://github.com/SEBASBELMOS/workshop-002/blob/main/assets/ip_config_gcp.png" width="300"/>

    - After you have done that, you are ready to create it.

4. **Connecting to the instance**
    Once the instance is created and running, we will test the connection with a DB Client such as pgAdmin, psql or DBeaver:
    
    - We need to provide these details:
        - Host: The server's public IP.
        - Port: 5432 (Default).
        - Username: This is the username you selected in the configuration (If you chose to use the default, then it is **`postgres`**)
        - Password: The password you created in GCP for this instance.
        - Database name: As we have not created a database you can test with the default database (**`postgres`**)

            <img src="https://github.com/SEBASBELMOS/workshop-002/blob/main/assets/db_test.png" width="300"/>
            <img src="https://github.com/SEBASBELMOS/workshop-002/blob/main/assets/connection_successful.png" width="300"/>
