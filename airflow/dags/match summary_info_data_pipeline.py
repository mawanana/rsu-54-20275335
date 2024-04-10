import re
import pandas as pd
from datetime import datetime
import requests
from bs4 import BeautifulSoup

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from includes.mysql_query_executor import get_matchs_data, insert_match_summary_data


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
    'match_summary_info_data_pipeline',
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
def get_match_url():
    df = get_matchs_data()
    return df

task_01 = PythonOperator(
    task_id='get_match_url',
    python_callable=get_match_url,
    dag=dag,
)

# Task 02: Scrape data from URL
def score_data_scrapeing(**kwargs):
    df = kwargs['task_instance'].xcom_pull(task_ids='get_match_url')
    scraped_data = []
    # -----use for test propo-----
    # # Slice the DataFrame to only include rows with index from 2000 to 2100
    # df = df.iloc[2000:2201]
    # # -----use for test propo-----

    for index, row in df.iterrows():
        url = 'https://www.espncricinfo.com{}'.format(row['url'])
        print(url)
        # Send a GET request to the URL
        response = requests.get(url)

        # Check if the request was successful (status code 200)
        if response.status_code == 200:
            # Parse the HTML content of the page
            soup = BeautifulSoup(response.content, "html.parser")

            # Find all <div> tags with the specified class
            score_div_tag = soup.find("div", class_="ds-flex ds-flex-col ds-mt-3 md:ds-mt-0 ds-mt-0 ds-mb-1")
            team_score_div_tags = score_div_tag.find_all("div",
                                                         class_="ci-team-score ds-flex ds-justify-between ds-items-center ds-text-typo ds-mb-1")
            if len(team_score_div_tags) == 2:
                bat_first_team_element = team_score_div_tags[0]
                bat_second_team_element = team_score_div_tags[1]
            else:
                bat_first_team_element = soup.find("div",
                                                   class_="ci-team-score ds-flex ds-justify-between ds-items-center ds-text-typo ds-opacity-50 ds-mb-1")
                bat_second_team_element = team_score_div_tags[0]

            print(bat_first_team_element.get_text(strip=True))
            print(bat_second_team_element.get_text(strip=True))
        else:
            print("Failed to retrieve the webpage. Status code:", response.status_code)

        if row['team_1'] in bat_first_team_element.get_text(strip=True):
            team_1_string = bat_first_team_element.get_text(strip=True)
            team_2_string = bat_second_team_element.get_text(strip=True)
        else:
            team_2_string = bat_first_team_element.get_text(strip=True)
            team_1_string = bat_second_team_element.get_text(strip=True)

        scraped_data.append([row['Id'], row['match_id'], row['team_1'], row['team_2'], row['url'], team_1_string, team_2_string])
    # Define column headers
    columns = ['Id', 'match_id', 'team_1', 'team_2', 'url', 'team_1_string', 'team_2_string']

    # Convert to DataFrame
    scraped_df = pd.DataFrame(scraped_data, columns=columns)
    print(scraped_df)
    return scraped_df

task_02 = PythonOperator(
    task_id='score_data_scrapeing',
    python_callable=score_data_scrapeing,
    dag=dag,
)

# Task 03: Transform data
def transform_data(**kwargs):
    scraped_df = kwargs['ti'].xcom_pull(task_ids='score_data_scrapeing')
    transformed_data =[]

    for index, row in scraped_df.iterrows():
        print(row['team_1_string'] , "========", row['team_2_string'])
        if ' ov' in row['team_1_string']:
            string = row['team_1_string'].split(' ov')[0].split('(')[1].strip()
            if '/' in string:
                team_1_over = string.split('/')[0].strip()
                team_1_total_over = string.split('/')[1].strip()
            else:
                team_1_over = string.strip()
                team_1_total_over = '20'
            string = row['team_1_string'].split(' ov')[1].split(')')[1].strip()
            if '/' in string:
                team_1_score = int(string.split('/')[0].strip())
                team_1_wicket = int(string.split('/')[1].strip())
            else:
                team_1_score = int(string.strip())
                team_1_wicket = 10
        elif not any(char.isdigit() for char in row['team_1_string']):
            team_1_score = 0
            team_1_wicket = 0
            team_1_over = '0'
            team_1_total_over = '0'
        else:
            string = row['team_1_string']
            if '/' in string:
                team_1_score = string.split('/')[0]
                team_1_score = int(re.sub(r'\D', '', team_1_score).strip())
                team_1_wicket = int(string.split('/')[1].strip())
                team_1_over = '20'
                team_1_total_over = '20'
            else:
                team_1_score = string.split('/')[0]
                team_1_score = int(re.sub(r'\D', '', team_1_score).strip())
                team_1_wicket = 10
                team_1_over = '20'
                team_1_total_over = '20'

        if ' ov' in row['team_2_string']:
            string = row['team_2_string'].split(' ov')[0].split('(')[1].strip()
            if '/' in string:
                team_2_over = string.split('/')[0].strip()
                team_2_total_over = string.split('/')[1].strip()
            else:
                team_2_over = string.strip()
                team_2_total_over = '20'
            string = row['team_2_string'].split(' ov')[1].split(')')[1].strip()
            if '/' in string:
                team_2_score = int(string.split('/')[0].strip())
                team_2_wicket = int(string.split('/')[1].strip())
            else:
                team_2_score = int(string.strip())
                team_2_wicket = 10
        elif not any(char.isdigit() for char in row['team_2_string']):
            team_2_score = 0
            team_2_wicket = 0
            team_2_over = '0'
            team_2_total_over = '0'
        else:
            string = row['team_2_string']
            if '/' in string:
                team_2_score = string.split('/')[0]
                team_2_score = int(re.sub(r'\D', '', team_2_score).strip())
                team_2_wicket = int(string.split('/')[1].strip())
                team_2_over = '20'
                team_2_total_over = '20'
            else:
                team_2_score = string.split('/')[0]
                team_2_score = int(re.sub(r'\D', '', team_2_score).strip())
                team_2_wicket = 10
                team_2_over = '20'
                team_2_total_over = '20'

        if 'T:' in row['team_1_string']:
            first_bat_team = row['team_2']
            second_bat_team = row['team_1']
        else:
            first_bat_team = row['team_1']
            second_bat_team = row['team_2']

        transformed_data.append([row['Id'], team_1_score, team_1_wicket, team_1_over, team_1_total_over, team_2_score, team_2_wicket, team_2_over, team_2_total_over, first_bat_team, second_bat_team])
    # Define column headers
    columns = ['Id', 'team_1_score', 'team_1_wicket', 'team_1_over', 'team_1_total_over', 'team_2_score', 'team_2_wicket', 'team_2_over', 'team_2_total_over', 'first_bat_team', 'second_bat_team']

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
    insert_match_summary_data(transformed_df)

task_04 = PythonOperator(
    task_id='insert_transformed_data_to_mysql',
    python_callable=insert_transformed_data_to_mysql,
    provide_context=True,
    dag=dag,
)

# Define task dependencies
task_01 >> task_02 >> task_03 >> task_04
