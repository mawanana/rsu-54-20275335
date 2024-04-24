import re
import requests
import pandas as pd
from datetime import datetime
from bs4 import BeautifulSoup

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from includes.match_name import find_most_matching_name
from includes.mysql_query_executor import get_matchs_data_batting, insert_batting_data


# Define default arguments
default_args = {
    'owner': 'rsu-54-20275335',
    'depends_on_past': False,
    'start_date': datetime(2024, 4, 10),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

# Define DAG
dag = DAG(
    'batting_info_data_pipeline',
    default_args=default_args,
    description='A DAG to handle match summary info data pipeline',
    schedule_interval=None,  # This example runs manually
    catchup=False,
    max_active_runs=1,
    concurrency=1,
    dagrun_timeout=None,
    tags=['match summary', 'pipeline'],
)

# Task 01: Get DataFrame data
def get_match_info():
    df = get_matchs_data_batting()
    print(df)
    return df

task_01 = PythonOperator(
    task_id='get_match_info',
    python_callable=get_match_info,
    dag=dag,
)

# Task 02: Scrape data from URL
def batting_data_scrapeing(**kwargs):
    df = kwargs['task_instance'].xcom_pull(task_ids='get_match_info')
    print(df.columns)

    scraped_data = []
    # # -----use for test propo-----
    # # Slice the DataFrame to only include rows with index from 2000 to 2100
    # df = df.iloc[2350:2440]
    # # -----use for test propo-----
    for index1, row1 in df.iterrows():
        url = 'https://www.espncricinfo.com{}'.format(row1['url'])
        print("##############", row1['match_id'],"--->", url)

        # Send a GET request to the URL
        response = requests.get(url)

        # Check if the request was successful (status code 200)
        if response.status_code == 200:
            # Parse the HTML content of the page
            soup = BeautifulSoup(response.content, "html.parser")

            # Find all tables with the specified class
            scorecard_tables = soup.find_all('table', class_='ci-scorecard-table')

            # Loop through each scorecard table
            for index, table in enumerate(scorecard_tables):

                # Get all table rows (excluding the first one, which usually contains column headers)
                rows = table.find_all('tr')[1:]

                # List to store valid rows of data
                data_list = []
                batting_position = 0

                # Loop through each row
                for row in rows:
                    # Get all table data cells in this row
                    cells = row.find_all('td')
                    if '†' in cells[0].get_text(strip=True):
                        wicket_keeper = cells[0].get_text(strip=True)
                        if ',' in wicket_keeper:
                            wicket_keeper = wicket_keeper.split('†')[0].split(',')[-1].strip()
                        wicket_keeper = wicket_keeper.replace("(c)", "").strip()
                        wicket_keeper = wicket_keeper.replace("†", "").strip()
                        wicket_keeper = wicket_keeper.replace("Did not bat:", "").strip()

                    if '(c)' in cells[0].get_text(strip=True):
                        captain = cells[0].get_text(strip=True)
                        if ',' in captain:
                            captain = captain.split('(c)')[0].split(',')[-1].strip()
                        captain = captain.replace("†", "").strip()
                        captain = captain.replace("(c)", "").strip()
                        captain = captain.replace("Did not bat:", "").strip()


                    # Extract text from each cell and store in variables
                    data = [cell.text.strip() for cell in cells]

                    # Check if the row is valid (not 'Extras', 'TOTAL', and has 8 elements)
                    if len(data) == 8 and data[0] not in ['Extras', 'TOTAL']:
                        batting_position += 1
                        # add batting_position
                        data.append(str(batting_position))
                        # add batman url
                        data.append(row.find('a').get('href').strip())
                        # Append the valid row to the data list
                        data_list.append(data)
                    elif len(data) == 1 and 'Did not bat:' in data[0]:
                        profile_url_list = row.find_all('a')

                        # Split the string by 'Did not bat:' and extract the second part
                        not_bat_players = data[0].split('Did not bat:')[1].strip()
                        # Split the extracted part by ',' and load the elements into a list
                        not_bat_players_list = [player.strip() for player in not_bat_players.split(',')]

                        # Combine players_list and profile_url_list
                        combined_list = zip(not_bat_players_list, profile_url_list)

                        # Extend data_list with the combined elements
                        data_list.extend(
                            [[player, 'Did not bat', '-', '-', '-', '-', '-', '-', '-', profile_url.get('href').strip()] for
                             player, profile_url in
                             combined_list])

                if index == 0:
                    team_name = row1['first_bat_team']
                    opposite_team  = row1['second_bat_team']
                if index == 1:
                    team_name = row1['second_bat_team']
                    opposite_team  = row1['first_bat_team']

                for sublist in data_list:
                    sublist.extend([team_name, opposite_team, captain, wicket_keeper, row1['match_id'], row1['url']])
                    # print(sublist)
                    scraped_data.append(sublist)

        else:
            print("Failed to retrieve the webpage. Status code:", response.status_code)

    # Define column headers
    columns = ['player', 'dismissal', 'runs', 'balls', 'minutes', 'fours', 'sixes', 'strike_rate', 'batting_position', 'profile_url', 'team', 'opposite_team', 'captain', 'wicket_keeper', 'match_id', 'url']

    # Convert to DataFrame
    scraped_df = pd.DataFrame(scraped_data, columns=columns)
    # print(scraped_df)
    return scraped_df

task_02 = PythonOperator(
    task_id='batting_data_scrapeing',
    python_callable=batting_data_scrapeing,
    dag=dag,
)

# Task 03: Transform data
def transform_data(**kwargs):
    scraped_df = kwargs['ti'].xcom_pull(task_ids='batting_data_scrapeing')
    transformed_data =[]
    print(scraped_df)
    for index, row in scraped_df.iterrows():
        print("========>>>>>", row['match_id'])
        print(row['dismissal'])
        players_list = scraped_df[scraped_df['match_id'] == row['match_id']]['player'].unique()
        print(players_list)

        if 'Did not bat' in row['dismissal']:
            dismissal_method = '-'
            bowler = '-'
            dismissal_participate_player = '-'
        if 'not out' in row['dismissal']:
            dismissal_method = 'not out'
            bowler = '-'
            dismissal_participate_player = '-'
        if 'b ' in row['dismissal']:
            dismissal_method = 'bowled'
            bowler = row['dismissal'].split('b ')[1].strip()
            bowler = find_most_matching_name(bowler, players_list)
            dismissal_participate_player = '-'
        if 'lbw ' in row['dismissal']:
            dismissal_method = 'leg before wicket'
            bowler = row['dismissal'].split('b ')[1].strip()
            bowler = find_most_matching_name(bowler, players_list)
            dismissal_participate_player = '-'
        if 'c ' in row['dismissal']:
            dismissal_method = 'caught'
            bowler = row['dismissal'].split('b ')[1].strip()
            bowler = find_most_matching_name(bowler, players_list)
            dismissal_participate_player = row['dismissal'].split('b ')[0].split('c ')[1].strip()
            dismissal_participate_player = find_most_matching_name(dismissal_participate_player, players_list)
        if 'run out' in row['dismissal']:
            dismissal_method = 'run out'
            bowler = '-'
            if '(' in row['dismissal']:
                dismissal_participate_player = row['dismissal'].split('(')[1].split(')')[0].strip()
                dismissal_participate_player = find_most_matching_name(dismissal_participate_player, players_list)
            else:
                dismissal_participate_player = '-'
        if 'st ' in row['dismissal']:
            dismissal_method = 'stumped'
            bowler = row['dismissal'].split('b ')[1].strip()
            bowler = find_most_matching_name(bowler, players_list)
            dismissal_participate_player = row['dismissal'].split('b ')[0].split('st ')[1].strip()
            dismissal_participate_player = find_most_matching_name(dismissal_participate_player, players_list)
        print(row['dismissal'], ">>>>>", dismissal_method, "====", bowler, "====", dismissal_participate_player)


        player = row['player']
        player = player.replace("(c)", "").strip()
        player = player.replace("†", "").strip()

        bowler = bowler.replace("(c)", "").strip()
        bowler = bowler.replace("†", "").strip()

        dismissal_participate_player = dismissal_participate_player.replace("(c)", "").strip()
        dismissal_participate_player = dismissal_participate_player.replace("†", "").strip()

        transformed_data.append([row['match_id'], row['team'], row['opposite_team'], player, row['profile_url'], row['batting_position'], row['captain'], row['wicket_keeper'], dismissal_method, bowler, dismissal_participate_player, row['runs'], row['balls'], row['minutes'], row['fours'], row['sixes'], row['strike_rate']])

    # Define column headers
    columns = ['match_id', 'team', 'opposite_team', 'player', 'profile_url', 'batting_position', 'captain', 'wicket_keeper', 'dismissal_method', 'bowler', 'dismissal_participate_player', 'runs', 'balls', 'minutes', 'fours', 'sixes', 'strike_rate']

    # Convert to DataFrame
    transformed_df = pd.DataFrame(transformed_data, columns=columns)
    print(transformed_df)
    return transformed_df


task_03 = PythonOperator(
    task_id='transform_data',
    python_callable=transform_data,
    provide_context=True,
    dag=dag,
)

# Task 04: Insert transformed data to MySQL
def insert_transformed_data_to_mysql(**kwargs):
    transformed_df = kwargs['ti'].xcom_pull(task_ids='transform_data')
    insert_batting_data(transformed_df)

task_04 = PythonOperator(
    task_id='insert_transformed_data_to_mysql',
    python_callable=insert_transformed_data_to_mysql,
    provide_context=True,
    dag=dag,
)

# Define task dependencies
task_01 >> task_02 >> task_03 >> task_04
