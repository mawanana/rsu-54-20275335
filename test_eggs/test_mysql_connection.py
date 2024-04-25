import mysql.connector

def get_mysql_connection():
    # MySQL database connection parameters
    config = {
        'host': 'localhost',
        'user': 'airflow',
        'password': 'airflow',
        'database': 'cricket_info'
    }

    try:
        # Establish a connection to the MySQL database
        connection = mysql.connector.connect(**config)
        print("Connected to MySQL database successfully!")
        return connection
    except mysql.connector.Error as error:
        print(f"Error: {error}")
        return None

# Example usage:
mysql_connection = get_mysql_connection()
if mysql_connection:
    # Use the connection for database operations
    pass
else:
    # Handle connection failure
    pass
