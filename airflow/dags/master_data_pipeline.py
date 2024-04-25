import os
import json
import pandas as pd
from datetime import datetime

from airflow import DAG
from airflow.operators.python_operator import PythonOperator

from includes.mysql_query_executor import insert_master_data

# Define default arguments for the DAG
default_args = {
    'owner': 'rsu-54-20275335',
    'depends_on_past': False,
    'start_date': datetime(2024, 2, 20),
    'retries': 1,
}

# Create the Airflow DAG
dag = DAG(
    'master_data_pipeline',
    default_args=default_args,
    description='DAG for Match info pipeline',
    # schedule_interval='0 0 * * *',  # Run daily at midnight
    schedule_interval=None,  # Disable the schedule
    catchup=False,
    max_active_runs=1,
    concurrency=1,
    dagrun_timeout=None,
    tags=['master_data', 'pipeline'],
)


def master_data_process():

    # Directory containing JSON files
    directory = '/opt/airflow/json_data'
    # directory = '/opt/airflow/t20s_male_json'

    # List all JSON files in the directory
    json_files = [file for file in os.listdir(directory) if file.endswith('.json')]

    # Initialize an empty list to store match info
    match_info_list = []

    # Initialize an empty DataFrame to store the combined data
    combined_df = pd.DataFrame()

    # Loop through each JSON file
    for file in json_files:
        # Read JSON file
        with open(os.path.join(directory, file)) as f:
            data = json.load(f)

        try:
            match_type_number = 'T20I # {}'.format(data['info']['match_type_number'])
            # --------city----------
            try:
                city = data['info']['city']
            except:
                city = '-'
            # --------player_of_match----------
            try:
                player_of_match = data['info']['player_of_match']
            except:
                player_of_match = '-'
            # --------winner----------
            try:
                winner = data['info']['outcome']['winner']
            except:
                winner = '-'

            # ------------------
            team1 = data['info']['teams'][0]
            team2 = data['info']['teams'][1]

            team1_players = data['info']['players'][team1]
            team2_players = data['info']['players'][team2]

            # ------------------
            try:
                if 'bat' in data['info']['toss']['decision']:
                    if data['info']['toss']['winner'] == team1:
                        first_bat = team1
                        first_ball = team2
                    else:
                        first_bat = team2
                        first_ball = team1
            except KeyError as e:
                pass
            try:
                if 'field' in data['info']['toss']['decision']:
                    if data['info']['toss']['winner'] == team1:
                        first_bat = team2
                        first_ball = team1
                    else:
                        first_bat = team1
                        first_ball = team2
            except KeyError as e:
                pass

            # ------------------
            try:
                if 'wickets' in data['info']['outcome']['by']:
                    winner_method = 'wickets'
                    winner_margin = data['info']['outcome']['by']['wickets']
            except KeyError as e:
                pass
            try:
                if 'runs' in data['info']['outcome']['by']:
                    winner_method = 'runs'
                    winner_margin = data['info']['outcome']['by']['runs']
            except KeyError as e:
                pass
            try:
                if 'tie' in data['info']['outcome']['result']:
                    winner_method = 'tie'
                    winner_margin = 0
            except KeyError as e:
                pass
            try:
                if 'no result' in data['info']['outcome']['result']:
                    winner_method = 'no result'
                    winner_margin = 0
            except KeyError as e:
                pass

            # ------------------

            # Extract relevant information from the JSON data
            match_info = {
                'match_id': match_type_number,
                'date': data['info']['dates'][0],
                'city': city,
                'winner': winner,
                'winner_method': winner_method,
                'winner_margin': winner_margin,
                'total_overs': data['info']['overs'],
                'player_of_match': str(player_of_match),
                'team1': team1,
                'team2': team2,
                'team1_players': str(team1_players),
                'team2_players': str(team2_players),
                'first_bat': first_bat,
                'first_ball': first_ball,
                'venue': data['info']['venue']
            }
            # Append match info to list
            match_info_list.append(match_info)
            # print(match_info)

            # Extract innings data
            innings_data = []
            for inning in data['innings']:
                for over in inning['overs']:
                    for idx, delivery in enumerate(over['deliveries']):
                        # for delivery in over['deliveries']:
                        inning_info = {
                            'inning': inning['team'],
                            'over': over['over'],
                            'deliveries': idx + 1,
                            'batter': delivery['batter'],
                            'bowler': delivery['bowler'],
                            'runs_batter': delivery['runs']['batter'],
                            'extras': delivery['runs'].get('extras', 0) if isinstance(delivery['runs'], dict) else 0,
                            'total_runs': delivery['runs']['total']
                        }
                        if 'wickets' in delivery:
                            inning_info['wicket_player_out'] = delivery['wickets'][0]['player_out']
                            inning_info['wicket_kind'] = delivery['wickets'][0]['kind']
                        else:
                            inning_info['wicket_player_out'] = None
                            inning_info['wicket_kind'] = None
                        innings_data.append(inning_info)

            # Create DataFrame for the current match
            df = pd.DataFrame(innings_data)

            # Append the DataFrame to the combined DataFrame
            combined_df = pd.concat([combined_df, df], ignore_index=True)

        except KeyError as e:
            print(f"KeyError occurred while processing file {file}: {e}")

        # # Delete the JSON file after reading data
        # os.remove(os.path.join(directory, file))

    # Create DataFrame for match info
    match_info_df = pd.DataFrame(match_info_list)

    # Repeat match info for each record in combined DataFrame
    combined_df = pd.concat([match_info_df.reindex(combined_df.index, method='ffill'), combined_df], axis=1)

    # Print the column list of the combined DataFrame
    print("Column List:")
    print(combined_df.columns.tolist())

    # Write the combined DataFrame to a CSV file
    combined_df.to_csv('combined_data.csv', index=False)

    # Display the combined DataFrame
    print(combined_df)

    return combined_df


def matchs_info_injection(**kwargs):
    df = kwargs['task_instance'].xcom_pull(task_ids='master_data_process')
    insert_master_data(df)
    pass


# Define the tasks

master_data_process_task = PythonOperator(
    task_id='master_data_process',
    python_callable=master_data_process,
    provide_context=True,
    dag=dag,
)

matchs_info_injection_task = PythonOperator(
    task_id='matchs_info_injection',
    python_callable=matchs_info_injection,
    provide_context=True,
    dag=dag,
)

# Define the task dependencies
master_data_process_task >> matchs_info_injection_task
