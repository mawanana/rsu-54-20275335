import mysql.connector

def get_mysql_connection():
    """
    Establishes a connection to the MySQL database and returns the connection object.
    Modify the configuration parameters as per your MySQL setup.
    """
    config = {
        'user': 'airflow',
        'password': 'airflow',
        'host': 'localhost',  # or your MySQL container IP address
        'port': '3306',        # or the port you've mapped to your MySQL container
        'database': 'cricket_info',
        'raise_on_warnings': True
    }

    try:
        connection = mysql.connector.connect(**config)
        print("Connected to MySQL database")
        return connection
    except mysql.connector.Error as err:
        print(f"Error: {err}")
        return None
