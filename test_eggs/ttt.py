import re
import ast
import time
import pandas as pd
from bs4 import BeautifulSoup
from difflib import SequenceMatcher

from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager

def get_soup_opposite_team(url):
    try:
        # Setup Chrome WebDriver with automatic installation using ChromeDriverManager
        service = Service(ChromeDriverManager().install())

        # Configure Chrome options for headless mode
        options = Options()
        # options.add_argument("--headless=new")
        options.add_argument("--no-sandbox")  # Bypass OS security model
        options.add_argument("--disable-dev-shm-usage")  # Overcome limited resource problems

        # Initialize Chrome WebDriver with the provided service and options
        driver = webdriver.Chrome(service=service, options=options)

        # Open the URL
        driver.get(url)

        # Wait for the element to be clickable
        WebDriverWait(driver, 20).until(EC.element_to_be_clickable((By.CSS_SELECTOR, "ul.ds-flex.ds-flex-col.ds-text-typo-mid2.ds-justify-center.ds-overflow-ellipsis.ds-overflow-y-auto.ds-w-full.ds-grid.ds-grid-cols-1.ds-items-center.ds-gap-x-2.ds-max-h-96.ds-overflow-y-auto")))

        # Find the element with class name "ds-flex ds-flex-col ds-text-typo-mid2 ds-justify-center ds-overflow-ellipsis ds-overflow-y-auto ds-w-full ds-grid ds-grid-cols-1 ds-items-center ds-gap-x-2 ds-max-h-96 ds-overflow-y-auto" and tag name "ul"
        element = driver.find_element_by_css_selector("ul.ds-flex.ds-flex-col.ds-text-typo-mid2.ds-justify-center.ds-overflow-ellipsis.ds-overflow-y-auto.ds-w-full.ds-grid.ds-grid-cols-1.ds-items-center.ds-gap-x-2.ds-max-h-96.ds-overflow-y-auto")

        # Click on the element
        element.click()

        # Wait for some time for the element to be processed
        time.sleep(5)  # You can adjust the time as needed

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


get_soup_opposite_team("https://www.espncricinfo.com/series/icc-men-s-t20-world-cup-east-asia-pacific-qlf-2023-1383050/philippines-vs-vanuatu-11th-match-1383099/ball-by-ball-commentary")
