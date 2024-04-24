import pandas as pd

from airflow.hooks.mysql_hook import MySqlHook

def get_matchs_data_ball_by_ball():
    try:
        # Create a MySqlHook instance to get the MySQL connection
        mysql_hook = MySqlHook("mysql_conn_id")
        mysql_connection = mysql_hook.get_conn()

        # Create a cursor for database operations
        cursor = mysql_connection.cursor()

        # Define the SQL query to retrieve data from matches table
        matches_query = """
        SELECT match_id, first_bat_team, second_bat_team, url
        FROM cricket_info.matches
        WHERE harvest_status = 'match summary harvested';
        """

        # Execute the query to fetch matches data
        cursor.execute(matches_query)
        matches_rows = cursor.fetchall()

        # Close cursor
        cursor.close()

        # Convert the fetched matches data into a DataFrame
        matches_columns = ['match_id', 'first_bat_team', 'second_bat_team', 'url']
        matches_df = pd.DataFrame(matches_rows, columns=matches_columns)

        # Initialize empty lists to store player data
        first_bat_team_players = []
        second_bat_team_players = []

        # Iterate through matches data to fetch player data from batting table
        for index, row in matches_df.iterrows():
            match_id = row['match_id']
            first_bat_team = row['first_bat_team']
            second_bat_team = row['second_bat_team']

            # Define SQL queries for fetching player data for each team
            first_bat_query = f"""
            SELECT player
            FROM cricket_info.batting
            WHERE match_id = '{match_id}' AND team = '{first_bat_team}';
            """

            second_bat_query = f"""
            SELECT player
            FROM cricket_info.batting
            WHERE match_id = '{match_id}' AND team = '{second_bat_team}';
            """

            # Execute queries to fetch player data
            cursor = mysql_connection.cursor()
            cursor.execute(first_bat_query)
            first_bat_rows = cursor.fetchall()
            first_bat_players = [player[0] for player in first_bat_rows]
            first_bat_team_players.append(first_bat_players)

            cursor.execute(second_bat_query)
            second_bat_rows = cursor.fetchall()
            second_bat_players = [player[0] for player in second_bat_rows]
            second_bat_team_players.append(second_bat_players)

            # Close cursor
            cursor.close()

        # Add player data to matches DataFrame
        matches_df['first_bat_team_players'] = first_bat_team_players
        matches_df['second_bat_team_players'] = second_bat_team_players

        # Close database connection
        mysql_connection.close()
        print(matches_df)
        return matches_df

    except Exception as e:
        print(f"Error: {str(e)}")
        return None

def insert_bowling_wickets_data(dataframe):
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
            overs = row['overs']
            out_player = row['out_player']
            runs = row['runs']
            wicket_position = row['wicket_position']
            total_wickets = row['total_wickets']
            bowling_position = row['bowling_position']

            # Define the SQL query to insert data into the table
            sql_query = """
            INSERT INTO cricket_info.bowling_wickets (
                match_id, team, opposite_team, player, profile_url, bowling_position, overs,
                out_player, runs, wicket_position, total_wickets
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """

            # Execute the query
            cursor.execute(sql_query, (
                match_id, team, opposite_team, player, profile_url, bowling_position, overs,
                out_player, runs, wicket_position, total_wickets
            ))

        # Commit the transaction
        mysql_connection.commit()

        # Close cursor and database connection
        cursor.close()
        mysql_connection.close()

        print("Data inserted successfully.")

    except Exception as e:
        print(f"Error: {str(e)}")

def insert_bowling_data(dataframe):
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
            overs = row['overs']
            maidens = row['maidens']
            runs = row['runs']
            wickets = row['wickets']
            economy_rate = row['economy_rate']
            dot = row['dot']
            fours = row['fours']
            sixes = row['sixes']
            wide_balls = row['wide_balls']
            no_balls = row['no_balls']
            bowling_position = row['bowling_position']

            # Define the SQL query to insert data into the table
            sql_query = """
            INSERT INTO cricket_info.bowling (
                match_id, team, opposite_team, player, profile_url, overs, maidens,
                runs, wickets, economy_rate, dot, fours, sixes, wide_balls, no_balls, bowling_position
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """

            # Execute the query
            cursor.execute(sql_query, (
                match_id, team, opposite_team, player, profile_url, overs, maidens,
                runs, wickets, economy_rate, dot, fours, sixes, wide_balls, no_balls, bowling_position
            ))

        # Commit the transaction
        mysql_connection.commit()

        # Close cursor and database connection
        cursor.close()
        mysql_connection.close()

        print("Data inserted successfully.")

    except Exception as e:
        print(f"Error: {str(e)}")

def get_matchs_data_bowling():
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

