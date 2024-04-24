import re
import ast
import time
import pandas as pd
from bs4 import BeautifulSoup
from datetime import datetime
from difflib import SequenceMatcher

from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from includes.get_selenim_driver import get_soup
from includes.mysql_query_executor import get_matchs_data_ball_by_ball
# from includes.mysql_query_executor import get_matchs_data, insert_match_summary_data


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
    'ball_by_ball_info_data_pipeline',
    default_args=default_args,
    description='A DAG to handle match summary info data pipeline',
    schedule_interval=None,  # This example runs manually
    catchup=False,
    max_active_runs=1,
    concurrency=1,
    dagrun_timeout=None,
    tags=['ball_by_ball', 'pipeline'],
)

# Task 01: Get DataFrame data
def get_match_url():
    df = get_matchs_data_ball_by_ball()
    # Write DataFrame to a CSV file
    df.to_csv('example.csv', index=False)
    return df

task_01 = PythonOperator(
    task_id='get_match_info',
    python_callable=get_match_url,
    dag=dag,
)


def get_soup(url):
    print("--000--")
    try:
        print("--111--")

        # Setup Chrome WebDriver with automatic installation using ChromeDriverManager
        service = Service(ChromeDriverManager().install())

        # Configure Chrome options for headless mode
        options = Options()
        options.add_argument("--headless=new")
        options.add_argument("--no-sandbox")  # Bypass OS security model
        options.add_argument("--disable-dev-shm-usage")  # Overcome limited resource problems
        print("--222--")

        # Initialize Chrome WebDriver with the provided service and options
        driver = webdriver.Chrome(service=service, options=options)
        print("--333--")

        # Open the URL
        driver.get(url)
        print("--444--")

        # Scroll down the webpage 10 times, waiting 2 seconds between each scroll
        for _ in range(25):
            # Execute JavaScript to scroll down the page
            driver.execute_script("window.scrollBy(0, window.innerHeight)")

            # Wait for 2 seconds
            time.sleep(1)
        print("--555--")

        # Get the page source and convert it to BeautifulSoup
        page_source = driver.page_source
        soup = BeautifulSoup(page_source, "html.parser")
        print("--666--")

        # Quit the driver
        driver.quit()

        return soup

    except Exception as e:
        print(f"Error: {str(e)}")
        return None


def most_matching_name(names_list, string):
    # Initialize variables to keep track of the most matching name and its similarity score
    most_matching_name = None
    max_similarity = 0

    # Iterate through the names list
    for name in names_list:
        # Calculate similarity score between the name and the given string
        similarity = SequenceMatcher(None, name.lower(), string.lower()).ratio()

        # Update most matching name and its similarity score if the current name has higher similarity
        if similarity > max_similarity:
            most_matching_name = name
            max_similarity = similarity

    return most_matching_name

def find_first_fielding_position(string):
    sorted_fielding_positions = ['Short backward square leg', 'Deep backward square leg', 'Deep backward square leg',
                                 'Silly mid-off/mid-on', 'Forward short leg', 'Deep extra cover', 'Deep mid-wicket',
                                 'Deep square leg', 'Deep square leg', 'Deep midwicket', 'Backward point',
                                 'Short fine leg', 'Deep midwicket', 'Short mid-off', 'Silly mid-off', 'Wicketkeeper',
                                 'Silly mid-on', 'Silly mid-on', 'Extra cover', 'Silly point', 'Fourth slip',
                                 'Second slip', 'Silly point', 'Extra cover', 'Cover point', 'Silly point',
                                 'Mid-wicket', 'Square leg', 'Deep point', 'Deep cover', 'Square-leg', 'Third slip',
                                 'Cow corner', 'Deep cover', 'Square leg', 'First slip', 'Third man', 'Short leg',
                                 'Long stop', 'Third man', 'Midwicket', 'Short leg', 'Fine leg', 'Long leg', 'Long-off',
                                 'Leg slip', 'Fine leg', 'Long leg', 'Long off', 'Fly slip', 'Leg slip',
                                 'Mid-off', 'Long-on', 'Long-on', 'Long on', 'Sweeper', 'Mid-off', 'Mid-on', 'Bowler',
                                 'Mid-on', 'Gully', 'Point', 'Cover', 'Gully', 'Point', 'Cover', 'Slip']

    for position in sorted_fielding_positions:
        if position.lower() in string.lower():
            return position
    return '-'


# Task 02: Scrape data from URL
def score_data_scrapeing(**kwargs):
    df = kwargs['task_instance'].xcom_pull(task_ids='get_match_info')

    # Display the DataFrame
    print(df)
    scraped_data = []
    # -----use for test propo-----
    # Slice the DataFrame to only include rows with index from 2000 to 2100
    df = df.iloc[2445:2447]
    # -----use for test propo-----

    for index, row in df.iterrows():
        print("888888------>>>>>>>", row[0], row[3], row[1], row[4])
        data_list = []
        match_id = row['match_id']
        team = row['first_bat_team']
        opposite_team = row['second_bat_team']

        url_part = row[3].replace("full-scorecard", "ball-by-ball-commentary")

        url = 'https://www.espncricinfo.com{}'.format(url_part)
        print("##############", row['match_id'], "--->", url)
        # Call the method to open the URL in a headless browser
        soup = get_soup(url)
        print(soup.prettify())
        print("--777--")

        div_elements = soup.find_all("div", class_="lg:hover:ds-bg-ui-fill-translucent ds-hover-parent ds-relative")
        print("444--->",len(div_elements))

        # Print the text content of each found element
        for div_element in div_elements:
            print("--888--")

            span_element = div_element.find_all("span")
            ball = span_element[0].text.strip()

            runs_other = span_element[1].text.strip()
            if '•' == runs_other:
                runs = '0'
                other_info = '-'
            else:
                runs = re.sub(r'\D', '', runs_other)
                if runs == '':
                    runs = '0'
                other_info = re.sub(r'\d', '', runs_other)
                if other_info == '':
                    other_info = '-'

            bowler_batman = span_element[2].text.strip()
            if ' to ' in bowler_batman:
                # print(bowler_batman.split(' to ')[0].strip(), "--->>>>",row[4])
                bowling_player = most_matching_name(ast.literal_eval(row[4]), bowler_batman.split(' to ')[0].strip())
                batting_player = most_matching_name(ast.literal_eval(row[5]),
                                                    bowler_batman.split(' to ')[1].split(',')[0].strip())

            try:
                short_element = div_element.find("p", class_="ci-html-content")
                short = short_element.text.strip()
            except:
                short = ''
            print("###", short)
            if int(runs) > 1:
                field_position = find_first_fielding_position(short)
            elif int(runs) == 1 and other_info == '-':
                field_position = find_first_fielding_position(short)
            else:
                field_position = '-'

            print(
                [match_id, team, opposite_team, ball, runs, other_info, bowling_player, batting_player, field_position])

            data_list.append(
                [match_id, team, opposite_team, ball, runs, other_info, bowling_player, batting_player, field_position])
        print(data_list)

    scraped_df = ''
    return scraped_df

task_02 = PythonOperator(
    task_id='score_data_scrapeing',
    python_callable=score_data_scrapeing,
    dag=dag,
)

# # Task 03: Transform data
# def transform_data(**kwargs):
#     scraped_df = kwargs['ti'].xcom_pull(task_ids='score_data_scrapeing')
#     transformed_data =[]
#
#     for index, row in scraped_df.iterrows():
#         print(row['team_1_string'] , "========", row['team_2_string'])
#         if ' ov' in row['team_1_string']:
#             string = row['team_1_string'].split(' ov')[0].split('(')[1].strip()
#             if '/' in string:
#                 team_1_over = string.split('/')[0].strip()
#                 team_1_total_over = string.split('/')[1].strip()
#             else:
#                 team_1_over = string.strip()
#                 team_1_total_over = '20'
#             string = row['team_1_string'].split(' ov')[1].split(')')[1].strip()
#             if '/' in string:
#                 team_1_score = int(string.split('/')[0].strip())
#                 team_1_wicket = int(string.split('/')[1].strip())
#             else:
#                 team_1_score = int(string.strip())
#                 team_1_wicket = 10
#         elif not any(char.isdigit() for char in row['team_1_string']):
#             team_1_score = 0
#             team_1_wicket = 0
#             team_1_over = '0'
#             team_1_total_over = '0'
#         else:
#             string = row['team_1_string']
#             if '/' in string:
#                 team_1_score = string.split('/')[0]
#                 team_1_score = int(re.sub(r'\D', '', team_1_score).strip())
#                 team_1_wicket = int(string.split('/')[1].strip())
#                 team_1_over = '20'
#                 team_1_total_over = '20'
#             else:
#                 team_1_score = string.split('/')[0]
#                 team_1_score = int(re.sub(r'\D', '', team_1_score).strip())
#                 team_1_wicket = 10
#                 team_1_over = '20'
#                 team_1_total_over = '20'
#
#         if ' ov' in row['team_2_string']:
#             string = row['team_2_string'].split(' ov')[0].split('(')[1].strip()
#             if '/' in string:
#                 team_2_over = string.split('/')[0].strip()
#                 team_2_total_over = string.split('/')[1].strip()
#             else:
#                 team_2_over = string.strip()
#                 team_2_total_over = '20'
#             string = row['team_2_string'].split(' ov')[1].split(')')[1].strip()
#             if '/' in string:
#                 team_2_score = int(string.split('/')[0].strip())
#                 team_2_wicket = int(string.split('/')[1].strip())
#             else:
#                 team_2_score = int(string.strip())
#                 team_2_wicket = 10
#         elif not any(char.isdigit() for char in row['team_2_string']):
#             team_2_score = 0
#             team_2_wicket = 0
#             team_2_over = '0'
#             team_2_total_over = '0'
#         else:
#             string = row['team_2_string']
#             if '/' in string:
#                 team_2_score = string.split('/')[0]
#                 team_2_score = int(re.sub(r'\D', '', team_2_score).strip())
#                 team_2_wicket = int(string.split('/')[1].strip())
#                 team_2_over = '20'
#                 team_2_total_over = '20'
#             else:
#                 team_2_score = string.split('/')[0]
#                 team_2_score = int(re.sub(r'\D', '', team_2_score).strip())
#                 team_2_wicket = 10
#                 team_2_over = '20'
#                 team_2_total_over = '20'
#
#         if 'T:' in row['team_1_string']:
#             first_bat_team = row['team_2']
#             second_bat_team = row['team_1']
#         else:
#             first_bat_team = row['team_1']
#             second_bat_team = row['team_2']
#
#         transformed_data.append([row['Id'], team_1_score, team_1_wicket, team_1_over, team_1_total_over, team_2_score, team_2_wicket, team_2_over, team_2_total_over, first_bat_team, second_bat_team])
#     # Define column headers
#     columns = ['Id', 'team_1_score', 'team_1_wicket', 'team_1_over', 'team_1_total_over', 'team_2_score', 'team_2_wicket', 'team_2_over', 'team_2_total_over', 'first_bat_team', 'second_bat_team']
#
#     # Convert to DataFrame
#     transformed_df = pd.DataFrame(transformed_data, columns=columns)
#     print(transformed_df)
#     return transformed_df
#
#
# task_03 = PythonOperator(
#     task_id='transform_data',
#     python_callable=transform_data,
#     provide_context=True,
#     dag=dag,
# )
#
# # Task 04: Insert transformed data to MySQL
# def insert_transformed_data_to_mysql(**kwargs):
#     transformed_df = kwargs['ti'].xcom_pull(task_ids='transform_data')
#     insert_match_summary_data(transformed_df)
#
# task_04 = PythonOperator(
#     task_id='insert_transformed_data_to_mysql',
#     python_callable=insert_transformed_data_to_mysql,
#     provide_context=True,
#     dag=dag,
# )

# Define task dependencies
# task_01 >> task_02 >> task_03 >> task_04
task_01 >> task_02
