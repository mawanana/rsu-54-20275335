import time
from bs4 import BeautifulSoup

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
            time.sleep(2)

        # Get the page source and convert it to BeautifulSoup
        page_source = driver.page_source
        soup = BeautifulSoup(page_source, "html.parser")

        # Quit the driver
        driver.quit()

        return soup

    except Exception as e:
        print(f"Error: {str(e)}")
        return None