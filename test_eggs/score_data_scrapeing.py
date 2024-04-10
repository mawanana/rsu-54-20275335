import requests
from bs4 import BeautifulSoup

# URL of the webpage to scrape
url = "https://www.espncricinfo.com/series/england-in-australia-2022-23-1317467/australia-vs-england-3rd-t20i-1317488/full-scorecard"
url = "https://www.espncricinfo.com/series/icc-men-s-t20-world-cup-sub-regional-europe-a-qlf-2022-1321212/croatia-vs-sweden-6th-match-group-1-1321262/full-scorecard"
url = "https://www.espncricinfo.com/series/west-africa-trophy-2023-24-1400971/ghana-vs-rwanda-4th-match-1400975/full-scorecard"
# Send a GET request to the URL
response = requests.get(url)

# Check if the request was successful (status code 200)
if response.status_code == 200:
    # Parse the HTML content of the page
    soup = BeautifulSoup(response.content, "html.parser")

    # Find all <div> tags with the specified class
    score_div_tag = soup.find("div", class_="ds-flex ds-flex-col ds-mt-3 md:ds-mt-0 ds-mt-0 ds-mb-1")
    team_score_div_tags = score_div_tag.find_all("div", class_="ci-team-score ds-flex ds-justify-between ds-items-center ds-text-typo ds-mb-1")
    if len(team_score_div_tags) == 2:
        bat_first_team_element = team_score_div_tags[0]
        bat_second_team_element = team_score_div_tags[1]
    else:
        bat_first_team_element = soup.find("div", class_="ci-team-score ds-flex ds-justify-between ds-items-center ds-text-typo ds-opacity-50 ds-mb-1")
        bat_second_team_element = team_score_div_tags[0]

    print(bat_first_team_element.get_text(strip=True))
    print(bat_second_team_element.get_text(strip=True))
else:
    print("Failed to retrieve the webpage. Status code:", response.status_code)
