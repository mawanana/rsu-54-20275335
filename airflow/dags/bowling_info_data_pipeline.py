import re
import requests
import pandas as pd
from datetime import datetime
from bs4 import BeautifulSoup

from airflow import DAG
from airflow.operators.python_operator import PythonOperator

# from includes.match_name import find_most_matching_name
from includes.mysql_query_executor import get_matchs_data_bowling, insert_bowling_data

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
    'bowling_info_data_pipeline',
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
    df = get_matchs_data_bowling()
    print(df)
    return df

task_01 = PythonOperator(
    task_id='get_match_info',
    python_callable=get_match_info,
    dag=dag,
)

# Task 02: Scrape data from URL
def bowling_data_scrapeing(**kwargs):
    df = kwargs['task_instance'].xcom_pull(task_ids='get_match_info')
    print(df.columns)

    scraped_data = []
    # # -----use for test propo-----
    # # Slice the DataFrame to only include rows with index from 2000 to 2100
    # df = df.iloc[2350:2355]
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
            scorecard_tables = soup.find_all('table', class_='ds-w-full ds-table ds-table-md ds-table-auto')
            print(len(scorecard_tables))
            # Loop through each scorecard table
            for index, table in enumerate(scorecard_tables):

                # Get all table rows (excluding the first one, which usually contains column headers)
                rows = table.find_all('tr')[1:]

                # List to store valid rows of data
                data_list = []
                bowling_position = 0

                # Loop through each row
                for row in rows:
                    print("00--->", row.get_text(strip=True))
                    # Get all table data cells in this row
                    cells = row.find_all('td')

                    # Extract text from each cell and store in variables
                    data = [cell.text.strip() for cell in cells]

                    # Check if the row is valid (not 'Extras', 'TOTAL', and has 8 elements)
                    if len(data) == 11:
                        bowling_position += 1
                        # add bowling_position
                        data.append(str(bowling_position))
                        # add batman url
                        data.append(row.find('a').get('href').strip())
                        # Append the valid row to the data list
                        data_list.append(data)

                print(data_list)


                if index == 0:
                    team_name = row1['first_bat_team']
                    opposite_team  = row1['second_bat_team']
                if index == 1:
                    team_name = row1['second_bat_team']
                    opposite_team  = row1['first_bat_team']

                for sublist in data_list:
                    sublist.extend([team_name, opposite_team, row1['match_id']])
                    # print(sublist)
                    scraped_data.append(sublist)
                print("00", data_list)


        else:
            print("Failed to retrieve the webpage. Status code:", response.status_code)

    # Define column headers
    columns = ["player", "overs", "maidens", "runs", "wickets", "economy_rate", "dot", "fours", "sixes", "wide_balls", "no_balls", "bowling_position", "profile_url", "team", "opposite_team", "match_id"]

    # Convert to DataFrame
    scraped_df = pd.DataFrame(scraped_data, columns=columns)
    print(scraped_df)
    return scraped_df

task_02 = PythonOperator(
    task_id='bowling_data_scrapeing',
    python_callable=bowling_data_scrapeing,
    dag=dag,
)

# Task 04: Insert transformed data to MySQL
def insert_transformed_data_to_mysql(**kwargs):
    transformed_df = kwargs['ti'].xcom_pull(task_ids='bowling_data_scrapeing')
    insert_bowling_data(transformed_df)

task_04 = PythonOperator(
    task_id='insert_transformed_data_to_mysql',
    python_callable=insert_transformed_data_to_mysql,
    provide_context=True,
    dag=dag,
)

# Define task dependencies
task_01 >> task_02 >> task_04
