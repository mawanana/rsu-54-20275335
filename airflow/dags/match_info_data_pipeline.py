import pandas as pd
from datetime import datetime
import requests
from bs4 import BeautifulSoup

from airflow import DAG
from airflow.operators.python_operator import PythonOperator

from includes.mysql_query_executor import insert_matchs_data

# Define default arguments for the DAG
default_args = {
    'owner': 'rsu-54-20275335',
    'depends_on_past': False,
    'start_date': datetime(2024, 2, 20),
    'retries': 1,
}

# Create the Airflow DAG
dag = DAG(
    'match_info_data_pipeline',
    default_args=default_args,
    description='DAG for Match info pipeline',
    # schedule_interval='0 0 * * *',  # Run daily at midnight
    schedule_interval=None,  # Disable the schedule
    catchup=False,
    max_active_runs=1,
    concurrency=1,
    dagrun_timeout=None,
    tags=['data', 'pipeline'],
)


def scrape_matchs_info():
    base_url = "https://www.espncricinfo.com/records/year/team-match-results/"
    year_urls = []

    # Generate URLs for the years from 2005 to 2024
    for year in range(2005, 2025):
        year_url = f"{base_url}{year}-{year}/twenty20-internationals-3"
        year_urls.append(year_url)

    # Initialize an empty list to store row data
    data = []

    # Scrape Data for all years
    for url in year_urls:
        print(url)

        # Send a GET request to the URL
        response = requests.get(url)

        # Check if the request was successful (status code 200)
        if response.status_code == 200:
            # Parse the HTML content of the page
            soup = BeautifulSoup(response.content, "html.parser")

            # Find the table with class "ds-table" and extract its rows
            table = soup.find("table",
                              class_="ds-w-full ds-table ds-table-xs ds-table-auto ds-w-full ds-overflow-scroll ds-scrollbar-hide")
            if table:
                tbody = table.find("tbody")
                rows = tbody.find_all("tr")

                # Loop through each row and append its content to the data list
                for row in rows:
                    # Extract the data from each cell in the row
                    cells = row.find_all("td")
                    row_data = [cell.get_text(strip=True) for cell in cells]

                    # Extract link from last cell, if present
                    link = cells[-1].find("a")
                    if link:
                        row_data.append(link.get("href"))

                    # Append the row data to the data list
                    data.append(row_data)

            else:
                print("Table not found on the webpage.")
        else:
            print("Failed to retrieve the webpage. Status code:", response.status_code)

    # Convert the data list into a DataFrame
    df = pd.DataFrame(data, columns=["team_1", "team_2", "winner", "margin", "ground", "match_date", "match_id", "url"])
    return df


def matchs_info_injection(**kwargs):
    df = kwargs['task_instance'].xcom_pull(task_ids='scrape_matchs_info')
    insert_matchs_data(df, "matches")
    pass


# Define the tasks

scrape_matchs_info_task = PythonOperator(
    task_id='scrape_matchs_info',
    python_callable=scrape_matchs_info,
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
scrape_matchs_info_task >> matchs_info_injection_task
