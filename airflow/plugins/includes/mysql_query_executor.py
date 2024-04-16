import pandas as pd

from airflow.hooks.mysql_hook import MySqlHook

def insert_batting_data(dataframe):
    try:
        # Create a MySqlHook instance to get the MySQL connection
        mysql_hook = MySqlHook("mysql_conn_id")
        mysql_connection = mysql_hook.get_conn()

        # Create a cursor for database operations
        cursor = mysql_connection.cursor()

        # Iterate over each row in the DataFrame
        for index, row in dataframe.iterrows():
            # Extract data from the DataFrame row
            match_id = row['match_id']
            team = row['team']
            opposite_team = row['opposite_team']
            player = row['player']
            profile_url = row['profile_url']
            batting_position = row['batting_position']
            captain = row['captain']
            wicket_keeper = row['wicket_keeper']
            dismissal_method = row['dismissal_method']
            bowler = row['bowler']
            dismissal_participate_player = row['dismissal_participate_player']
            runs = row['runs']
            balls = row['balls']
            minutes = row['minutes']
            fours = row['fours']
            sixes = row['sixes']
            strike_rate = row['strike_rate']

            # Define the SQL query to insert data into the table
            sql_query = """
            INSERT INTO cricket_info.batting (
                match_id, team, opposite_team, player, profile_url, batting_position,
                captain, wicket_keeper, dismissal_method, bowler, dismissal_participate_player,
                runs, balls, minutes, fours, sixes, strike_rate
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """

            # Execute the query
            cursor.execute(sql_query, (
                match_id, team, opposite_team, player, profile_url, batting_position,
                captain, wicket_keeper, dismissal_method, bowler, dismissal_participate_player,
                runs, balls, minutes, fours, sixes, strike_rate
            ))

        # Commit the transaction
        mysql_connection.commit()

        # Close cursor and database connection
        cursor.close()
        mysql_connection.close()

        print("Data inserted successfully.")

    except Exception as e:
        print(f"Error: {str(e)}")

def get_matchs_data_batting():
    try:
        # Create a MySqlHook instance to get the MySQL connection
        mysql_hook = MySqlHook("mysql_conn_id")
        mysql_connection = mysql_hook.get_conn()

        # Create a cursor for database operations
        cursor = mysql_connection.cursor()

        # Define the SQL query to retrieve data
        sql_query = """
        SELECT match_id, first_bat_team, second_bat_team, url
        FROM cricket_info.matches
        WHERE harvest_status = 'match summary harvested';
        """

        # Execute the query
        cursor.execute(sql_query)

        # Fetch all rows from the result set
        rows = cursor.fetchall()

        # Close cursor and database connection
        cursor.close()
        mysql_connection.close()

        # Convert the fetched data into a DataFrame
        columns = ['match_id', 'first_bat_team', 'second_bat_team', 'url']
        df = pd.DataFrame(rows, columns=columns)

        return df

    except Exception as e:
        print(f"Error: {str(e)}")
        return None

def insert_match_summary_data(dataframe):
    # Create a MySqlHook instance to get the MySQL connection
    mysql_hook = MySqlHook("mysql_conn_id")
    mysql_connection = mysql_hook.get_conn()

    try:
        # Create a cursor for database operations
        cursor = mysql_connection.cursor()

        # Iterate over DataFrame rows and insert data into the MySQL table
        for _, row in dataframe.iterrows():
            columns = ['Id', 'team_1_score', 'team_1_wicket', 'team_1_over', 'team_1_total_over', 'team_2_score',
                       'team_2_wicket', 'team_2_over', 'team_2_total_over', 'first_bat_team', 'second_bat_team']

            # Define the SQL query for insertion with WHERE condition on 'Id'
            insert_query = """
                UPDATE cricket_info.matches
                SET team_1_score = %s, team_1_wicket = %s, team_1_over = %s,  team_1_total_over = %s,
                    team_2_score = %s, team_2_wicket = %s, team_2_over = %s,   team_2_total_over = %s,
                    first_bat_team = %s, second_bat_team = %s, harvest_status = %s
                WHERE Id = %s
            """

            # Extracting necessary values from the row
            values = (
                row['team_1_score'], row['team_1_wicket'], row['team_1_over'], row['team_1_total_over'],
                row['team_2_score'], row['team_2_wicket'], row['team_2_over'], row['team_2_total_over'],
                row['first_bat_team'], row['second_bat_team'], 'match summary harvested',
                row['Id']
            )

            # Execute the SQL query
            cursor.execute(insert_query, values)

        # Commit the changes to the database
        mysql_connection.commit()

    except Exception as e:
        print(f"Error: {str(e)}")

    finally:
        # Close cursor and database connection
        cursor.close()
        mysql_connection.close()

def get_matchs_data():
    try:
        # Create a MySqlHook instance to get the MySQL connection
        mysql_hook = MySqlHook("mysql_conn_id")  # Replace "your_mysql_conn_id" with your connection ID
        mysql_connection = mysql_hook.get_conn()

        # Create a cursor for database operations
        cursor = mysql_connection.cursor()

        # Define the SQL query to retrieve data
        sql_query = """
        SELECT Id, match_id, team_1, team_2, url
        FROM cricket_info.matches
        WHERE harvest_status IS NULL;
        """

        # Execute the query
        cursor.execute(sql_query)

        # Fetch all rows from the result set
        rows = cursor.fetchall()

        # Close cursor and database connection
        cursor.close()
        mysql_connection.close()

        # Convert the fetched data into a DataFrame
        columns = ['Id', 'match_id', 'team_1', 'team_2', 'url']
        df = pd.DataFrame(rows, columns=columns)

        return df

    except Exception as e:
        print(f"Error: {str(e)}")
        return None

def insert_matchs_data(dataframe, table_name):
  
    # Create a MySqlHook instance to get the MySQL connection
    mysql_hook = MySqlHook("mysql_conn_id")
    mysql_connection = mysql_hook.get_conn()

    try:
        # Create a cursor for database operations
        cursor = mysql_connection.cursor()

        # # Define the SQL query to retrieve the maximum batch_id
        # sql_query = "SELECT MAX(batch_id) AS max_batch_id FROM social_media_db.youtube_channel_data;"
        #
        # # Execute the query
        # cursor.execute(sql_query)
        #
        # # Fetch the result (maximum batch_id)
        # max_batch_id = cursor.fetchone()[0]
        # if max_batch_id is None:
        #     max_batch_id = 1
        # else:
        #     max_batch_id += 1  # Increment the max_batch_id by 1

        # Iterate over DataFrame rows and insert data into the MySQL table
        for _, row in dataframe.iterrows():
            insert_query = """INSERT INTO matches (team_1, team_2, winner, margin, ground, match_date, match_id, url)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
"""
            values = (row['team_1'], row['team_2'], row['winner'], row['margin'], row['ground'],
                row['match_date'], row['match_id'], row['url'])

            cursor.execute(insert_query, values)

        # Commit the changes to the database
        mysql_connection.commit()

    except Exception as e:
        print(f"Error: {str(e)}")

    finally:
        # Close cursor and database connection
        cursor.close()
        mysql_connection.close()

