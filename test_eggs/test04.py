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
from selenium.webdriver.common.keys import Keys  # Import Keys module
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.common.by import By
from selenium.webdriver.common.action_chains import ActionChains

def get_soup_opposite_team(url):
    try:
        service = Service(ChromeDriverManager().install())
        options = Options()
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")
        driver = webdriver.Chrome(service=service, options=options)

        driver.get(url)
        time.sleep(10)

        # Find the element using XPath
        element = driver.find_element("xpath", '//*[@id="main-container"]/div[5]/div[1]/div/div[3]/div[1]/div[2]/div[1]/div[1]/div/div[2]/div')
        element.click()

        time.sleep(1)
        print("=====1111======")
        time.sleep(20)

        # Find the element you want to move the cursor to
        dp_element = driver.find_element(By.XPATH, '//*[@id="tippy-14"]/div/div/div/div/div/ul/li[1]/div/span')

        # Move the cursor to the element
        actions = ActionChains(driver)
        actions.send_keys(Keys.ARROW_DOWN).perform()
        print("=====2222======")
        time.sleep(20)
        actions.move_to_element(dp_element).click().perform()

        # actions.move_by_offset(100, 200).click().perform()

        # # Simulate pressing the down arrow key twice to select the second option
        # actions = ActionChains(driver)
        # actions.send_keys(Keys.ARROW_DOWN).perform()
        # # actions.send_keys(Keys.ENTER).perform()

        print("===========")
        time.sleep(100)


        for _ in range(25):
            driver.execute_script("window.scrollBy(0, window.innerHeight)")
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

get_soup_opposite_team("https://www.espncricinfo.com/series/icc-men-s-t20-world-cup-east-asia-pacific-qlf-2023-1383050/philippines-vs-vanuatu-11th-match-1383099/ball-by-ball-commentary")
