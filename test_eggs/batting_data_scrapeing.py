import requests
import pandas as pd
from bs4 import BeautifulSoup

# URL of the webpage to scrape
url = "https://www.espncricinfo.com/series/england-in-australia-2022-23-1317467/australia-vs-england-3rd-t20i-1317488/full-scorecard"
url = "https://www.espncricinfo.com/series/icc-men-s-t20-world-cup-sub-regional-europe-a-qlf-2022-1321212/croatia-vs-sweden-6th-match-group-1-1321262/full-scorecard"
url = "https://www.espncricinfo.com/series/west-africa-trophy-2023-24-1400971/ghana-vs-rwanda-4th-match-1400975/full-scorecard"
# Send a GET request to the URL
response = requests.get(url)

# Check if the request was successful (status code 200)
# Check if the request was successful (status code 200)
if response.status_code == 200:
    # Parse the HTML content of the page
    soup = BeautifulSoup(response.content, "html.parser")

    # Find all tables with the specified class
    scorecard_tables = soup.find_all('table', class_='ci-scorecard-table')

    # List to store valid rows of data
    data_list = []
    profile_url_element_list = []
    profile_url_list = []

    # Loop through each scorecard table
    for index, table in enumerate(scorecard_tables):
        # Get all table rows (excluding the first one, which usually contains column headers)
        rows = table.find_all('tr')[1:]

        # Loop through each row
        for row in rows:

            # Get all table data cells in this row
            cells = row.find_all('td')

            # Extract text from each cell and store in variables
            data = [cell.text.strip() for cell in cells]

            # Check if the row is valid (not 'Extras', 'TOTAL', and has 8 elements)
            if len(data) == 8 and data[0] not in ['Extras', 'TOTAL']:
                data.append(row.find('a').get('href').strip())
                # Append the valid row to the data list
                data_list.append(data)
                print(data)
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
                    [[player, 'Did not bat', '-', '-', '-', '-', '-', '-', profile_url.get('href').strip()] for player, profile_url in
                     combined_list])

        if data in data_list:
            print(data_list)
            scraped_data_list = data_list.extend([])

    # # Convert the player list to a DataFrame
    # data_df = pd.DataFrame(data_list,
    #                   columns=['batsman', 'dismissal', 'runs', 'balls', 'minutes', 'fours', 'sixes', 'strike_rate', 'profile_url'])
    #
    # print(data_df)
else:
    print("Failed to retrieve the webpage. Status code:", response.status_code)