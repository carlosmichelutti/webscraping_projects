from _load_env import db_user, db_pass, db_host, db_port, db_name

from customized_package.utils import Database

from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import NoSuchElementException
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.common.by import By
from selenium import webdriver

from datetime import datetime, timedelta
from bs4 import BeautifulSoup
from colorama import Fore
from time import sleep
import pandas as pd
import requests
import aiocron
import asyncio
import sys
import os

class ScrapingPhonesSelenium:

    """
        Class that deals with the cell phone web scraping process using Selenium technology.

        Attributes
        ----------
            self.URL: str -> URL of the website to scraping;
            self.dataframe: pd.DataFrame -> Dataframe to store the scraped data;
            self.products: dict -> Dictionary with cell phone data scraped.

        Methods
        -------
            start -> Function that starts the scraping process;\n
            open_and_configure_web_driver -> Function that opens and configures selenium's automated driverweb;\n
            open_web_site -> Function that opens the web site;\n
            scraping_phones -> Function that scrapes the names and values ​​of each cell;\n
            create_spreadsheet -> Function that creates the spreadsheet with the scraped data.
    """
    
    def __init__(
            self: object
        ) -> None:

        self.URL = 'https://telefonesimportados.netlify.app'

        self.dataframe = pd.DataFrame()

        self.products = {'phone_name': [], 'original_value': [], 'discount_value': []}
    
    def start(self: object):

        """
            Function that starts the scraping process.
        """

        self.open_and_configure_web_driver()
        self.open_web_site()
        self.scraping_phones()
        self.create_spreadsheet()
    
    def open_and_configure_web_driver(self: object):

        """
            Function that opens and configures the web driver.

            Attributes
            ----------
                self.options -> Object with all webdriver settings;
                self.driver -> Instance of the webdriver automated and controlled by Selenium;
                self.wait -> Explicit wait instance of WebDriverWait;
        """

        self.options = webdriver.ChromeOptions()

        options = [
            '--incognito', 
            '--disable-component-extensions-with-background-pages',
            '--disable-background-networking',
            '--disable-notifications', 
            '--start-maximized'
        ]
    
        for option in options:
            self.options.add_argument(option)

        self.driver = webdriver.Chrome(options=self.options)
        self.wait = WebDriverWait(self.driver, 10, 1)

    def open_web_site(self: object):

        """
            Function that opens the web site.
        """
        
        self.driver.get(
            self.URL
        )

    def scraping_phones(self: object):

        """
            Function that scrapes the names and values ​​of each cell per page until there are no more pages.
        """

        page = 1
        while True:
            try:
                if self.driver.find_element(
                    By.CSS_SELECTOR, "body > div > div > div > h1"
                ).text == 'Page Not Found':
                    print(f'{Fore.RED}No more pages!{Fore.RESET}')
                    break
            except NoSuchElementException:
                print(f'{Fore.GREEN}Colecting data from page {page}{Fore.RESET}')

            self.wait.until(
                EC.visibility_of_all_elements_located(
                    (
                        By.CSS_SELECTOR, "div.single-shop-product"
                    )
                )
            )

            html_page = BeautifulSoup(
                self.driver.page_source, 'html.parser'
            )

            phones = html_page.select('div.single-product-area > div.container > div.row > div.col-md-3.col-sm-6')

            for phone in phones:
                self.products['phone_name'].append(phone.select_one("div.single-shop-product > h2 > a").get_text().strip())
                self.products['discount_value'].append(phone.select_one("div.single-shop-product > div:nth-child(3) > ins").get_text().strip())
                self.products['original_value'].append(phone.select_one("div.single-shop-product > div:nth-child(3) > del").get_text().strip())

            page += 1
            
            self.driver.get(
                self.URL + f'/shop{page}.html'
            )

    def create_spreadsheet(self: object):

        """
            Function that creates the spreadsheet with the scraped data.
        """

        dataframe = pd.DataFrame(self.products)
        self.dataframe = pd.concat([self.dataframe, dataframe], ignore_index=True)

class ScrapingPhonesRequests:

    """
        Class that deals with the cell phone web scraping process using Requests technology.

        Attributes
        ----------
            self.URL: str -> URL of the website to scraping;
            self.session: requests.Session -> requests session object to store the history of requests, cookies, etc;
            self.dataframe: pd.DataFrame -> Dataframe to store the scraped data;
            self.database: Database -> Database object to store the scraped data;
            self.products: dict -> Dictionary with cell phone data scraped.

        Methods
        -------
            start -> Function that starts the scraping process;\n
            get_pages -> Function that gets the number of pages to be scraped;\n
            scraping_phones -> Function that scrapes the names and values ​​of each cell;\n
            create_spreadsheet -> Function that creates the spreadsheet with the scraped data;\n
            clean_database -> Function that cleans the database to insert new data;\n
            insert_data_to_database -> Function that inserts the data into the database.
    """

    def __init__(
            self: object
        ) -> None:

        self.URL = 'https://telefonesimportados.netlify.app'

        self.session = requests.Session()
        self.session.headers.update(
            {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.36',
            }
        )

        self.dataframe = pd.DataFrame()

        self.database = Database(
            db_user=db_user,
            db_pass=db_pass,
            db_host=db_host,
            db_port=db_port,
            db_name=db_name,
        )

        self.products = {'phone_name': [],'original_value': [], 'discount_value': []}

    def start(self: object):

        """
            Function that starts the scraping process.
        """
        
        self.get_pages()
        self.scraping_phones()
        self.create_spreadsheet()
        self.clean_database()
        self.insert_data_to_database()

    def get_pages(self: object):

        """
            Function that obtains the number of pages to collect data.

            Exceptions
            ----------
                1. Exception - If it is not possible to obtain the number of pages to collect data, and within 3 attempts, an exception will be triggered.
        """

        tries = 0
        while True:
            if tries == 3:
                raise Exception(
                    'After 3 attempts it was not possible to obtain the number of pages to collect data.'
                )
            
            try:
                html_page = BeautifulSoup(
                    self.session.get(
                        self.URL
                    ).content, 'html.parser')
                
                self.pages = len(
                    html_page.select(
                        'ul.pagination > li > a:not([aria-label="Previous"]):not([aria-label="Next"])'
                    )
                )
            
            except Exception as e:
                print(f'Attempt {tries} to get the number of pages to collect data failed. Error: {e}. Trying again...')
                tries += 1
                sleep(5)
            else:
                break

    def scraping_phones(self: object):

        """
            Function that scrapes the names and values ​​of each cell phone going through each page.

            Exceptions
            ----------
                1. Exception - If it is not possible to collect data from a page, and within 3 attempts, an exception will be raised.
        """

        for page in range(1, self.pages + 1):
            print(f'{Fore.GREEN}Colecting data from page {page}{Fore.WHITE}')
            tries = 0
            while True:
                if tries == 3:
                    raise Exception(
                        f'After 3 attempts, it was not possible to collect data from the page {page}.'
                    )
                
                try:
                    html_page = BeautifulSoup(
                        self.session.get(
                            self.URL if page == 1 else self.URL + f'/shop{page}.html'
                        ).content, 'html.parser')

                    for phone in html_page.select('div.single-product-area > div.container > div.row > div.col-md-3.col-sm-6'):
                        self.products['phone_name'].append(phone.select_one("div.single-shop-product > h2 > a").get_text().strip())
                        self.products['discount_value'].append(phone.select_one("div.single-shop-product > div:nth-child(3) > ins").get_text().strip())
                        self.products['original_value'].append(phone.select_one("div.single-shop-product > div:nth-child(3) > del").get_text().strip())
                
                except Exception as e:
                    print(f'Attempt {tries} attempt to collect data from the page {page} failed. Error: {e}. Trying again...')
                    tries += 1
                    sleep(5)
                else:
                    break

        print(f'{Fore.RED}No more pages!{Fore.WHITE}')

    def create_spreadsheet(self: object):

        """
            Function that creates the spreadsheet with the scraped data.
        """

        dataframe = pd.DataFrame(self.products)
        self.dataframe = pd.concat([self.dataframe, dataframe], ignore_index=True)

    def clean_database(self: object):

        """
            Function that cleans the database.

            Exceptions
            ----------
                1. If it is not possible to clean the database, and within 3 attempts, an exception will be triggered.
        """

        tries = 0
        while True:
            if tries == 3:
                raise Exception(
                    'After 3 attempts, it was not possible to clean the database.'
                )
            
            try:
                self.database.execute_query_to_clean_database(
                    'TRUNCATE TABLE mydatabase.celphone_scraping;'
                )
            
            except Exception as e:
                print(f'Attempt {tries} to clean the database failed. Error: {e}. Trying again...')
                tries += 1
                sleep(5)
            else:
                break

    def insert_data_to_database(self: object):

        """
            Function that inserts the data into the database.

            Exceptions
            ----------
                1. If it is not possible to insert the data into the database, and within 3 attempts, an exception will be triggered.
        """

        tries = 0
        while True:
            if tries == 3:
                raise Exception(
                    'After 3 attempts, it was not possible to insert the data into the database.'
                )
            
            try:
                self.database.insert_data(
                    data=self.dataframe, table_name='celphone_scraping', if_exists='append'
                )
            
            except Exception as e:
                print(f'Attempt {tries} to insert the data into the database failed. Error: {e}. Trying again...')
                tries += 1
                sleep(5)
            else:
                break



if len(sys.argv) > 1:
    if datetime.now().second < 50:
        cron = (datetime.now() + timedelta(minutes=1 + int(sys.argv[1]))).strftime('%M %H * * *')
    else:
        cron = (datetime.now() + timedelta(minutes=2 + int(sys.argv[1]))).strftime('%M %H * * *')
else:
    if datetime.now().second < 50:
        cron = (datetime.now() + timedelta(minutes=1)).strftime('%M %H * * *')
    else:
        cron = (datetime.now() + timedelta(minutes=2)).strftime('%M %H * * *')

print(f'This script will start at {datetime.strptime(cron, "%M %H * * *").strftime("%H:%M")}.')

@aiocron.crontab(cron, start=True)
async def start_all_scraping_celphone():

    try:
        selenium_bot = ScrapingPhonesSelenium()
        selenium_bot.start()

    except Exception as e:
        print(f'Fail of the scraping process. Error: {e}.')
    else:
        print(f'Scraping process sucessfully.\n')
        selenium_bot.driver.quit()

    try:
        requests_bot = ScrapingPhonesRequests()
        requests_bot.start()

    except Exception as e:
        print(f'Fail of the scraping process. Error: {e}.')
    else:
        print(f'Scraping process sucessfully.\n')

    folder_directory = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    directory_report = folder_directory + os.sep + 'TMP' + os.sep + 'cel_phone_scraping'

    if not os.path.exists(directory_report):
        os.makedirs(directory_report)

    print(f'Saving the collected data in an excel report in the {Fore.GREEN}"{directory_report}"{Fore.RESET} directory.\n')
    with pd.ExcelWriter(directory_report + os.sep + 'phones_scraping.xlsx') as writer:
        selenium_bot.dataframe.to_excel(writer, index=False, sheet_name='phones_scraping_selenium')
        requests_bot.dataframe.to_excel(writer, index=False, sheet_name='phones_scraping_requests')


loop = asyncio.get_event_loop()

try:
    loop.run_forever()

except Exception:
    loop.close()