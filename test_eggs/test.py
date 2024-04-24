import re
import ast
import time
import pandas as pd
from bs4 import BeautifulSoup
from difflib import SequenceMatcher

from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager

def get_soup(url):
    try:
        # Setup Chrome WebDriver with automatic installation using ChromeDriverManager
        service = Service(ChromeDriverManager().install())

        # Configure Chrome options for headless mode
        options = Options()
        options.add_argument("--headless=new")
        options.add_argument("--no-sandbox")  # Bypass OS security model
        options.add_argument("--disable-dev-shm-usage")  # Overcome limited resource problems

        # Initialize Chrome WebDriver with the provided service and options
        driver = webdriver.Chrome(service=service, options=options)

        # Open the URL
        driver.get(url)
        # Scroll down the webpage 10 times, waiting 2 seconds between each scroll
        for _ in range(25):
            # Execute JavaScript to scroll down the page
            driver.execute_script("window.scrollBy(0, window.innerHeight)")

            # Wait for 2 seconds
            time.sleep(1)

        # Get the page source and convert it to BeautifulSoup
        page_source = driver.page_source
        soup = BeautifulSoup(page_source, "html.parser")

        # Quit the driver
        driver.quit()

        return soup

    except Exception as e:
        print(f"Error: {str(e)}")
        return None

def get_soup_opposite_team(url):
    try:
        # Setup Chrome WebDriver with automatic installation using ChromeDriverManager
        service = Service(ChromeDriverManager().install())

        # Configure Chrome options for headless mode
        options = Options()
        options.add_argument("--headless=new")
        options.add_argument("--no-sandbox")  # Bypass OS security model
        options.add_argument("--disable-dev-shm-usage")  # Overcome limited resource problems

        # Initialize Chrome WebDriver with the provided service and options
        driver = webdriver.Chrome(service=service, options=options)

        # Open the URL
        driver.get(url)


        # Scroll down the webpage 10 times, waiting 2 seconds between each scroll
        for _ in range(25):
            # Execute JavaScript to scroll down the page
            driver.execute_script("window.scrollBy(0, window.innerHeight)")

            # Wait for 2 seconds
            time.sleep(1)

        # Get the page source and convert it to BeautifulSoup
        page_source = driver.page_source
        soup = BeautifulSoup(page_source, "html.parser")

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

# Read CSV file into a DataFrame
df = pd.read_csv('/Users/eranda/Documents/MyDocuments/rsu-54-20275335/example.csv')

# Display the DataFrame
print(df)
scraped_data = []
# -----use for test propo-----
# Slice the DataFrame to only include rows with index from 2000 to 2100
df = df.iloc[2463:2465]
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

    div_elements = soup.find_all("div", class_="lg:hover:ds-bg-ui-fill-translucent ds-hover-parent ds-relative")

    # Print the text content of each found element
    for div_element in div_elements:
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
            batting_player = most_matching_name(ast.literal_eval(row[5]), bowler_batman.split(' to ')[1].split(',')[0].strip())

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

        print([match_id, team, opposite_team, ball, runs, other_info, bowling_player, batting_player, field_position])


        data_list.append([match_id, team, opposite_team, ball, runs, other_info, bowling_player, batting_player, field_position])
    print(data_list)