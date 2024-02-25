from _load_env import db_user, db_pass, db_host, db_port, db_name

from customized_package.utils import Database

from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.common.by import By
from selenium import webdriver

from datetime import datetime, timedelta
from bs4 import BeautifulSoup
from colorama import Fore
from time import sleep
import pandas as pd
import requests
import asyncio
import aiocron
import sys
import os
import re


class ScraplingFilmesSelenium:

    """
        Class that deals with the films scraping web scraping process using Selenium technology.

        Attributes
        ----------
            self.URL: str -> URL of the website to scraping;
            self.dataframe: pd.DataFrame -> Dataframe to store the scraped data;

        Methods
        -------
            start -> Function that starts the scraping process;\n
            open_and_configure_web_driver -> Function that opens and configures selenium's automated driverweb;\n
            open_web_site -> Function that opens the web site;\n
            scraping_films -> Function that scrapes the names, year of release, duration and rating of each film;\n
    """

    def __init__(
        self: object
    ) -> None:

        self.URL = 'https://www.imdb.com'

        self.dataframe = pd.DataFrame()

    def start(self: object):

        """
            Function that starts the scraping process.
        """

        self.open_and_configure_web_driver()
        self.open_web_site()
        self.scraping_films()

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
        self.wait = WebDriverWait(self.driver, 20, 1)

    def open_web_site(self: object):

        """
            Function that opens the web site.
        """

        self.driver.get(
            self.URL + '/chart/top/?ref_=nv_mv_250'
        )

    def scraping_films(self: object):

        """
            Function that scrapes the the names, year of release, duration and rating of each film.
        """

        self.wait.until(
            EC.element_to_be_clickable(
                (
                    By.CSS_SELECTOR, "div#ipc-wrap-background-id"
                )
            )
        )

        films = BeautifulSoup(
            self.driver.page_source, 'html.parser'
        ).select(
            "div.ipc-page-grid.ipc-page-grid--bias-left > div > ul > li"
        )

        for film in films:
            film_data={
                'film_name': film.select_one('div:nth-child(2) > div > div > div:nth-child(1) > a').get_text().split('.', 1)[-1].strip(),
                'year_film': film.select_one('div:nth-child(2) > div > div > div:nth-child(2) > span:nth-child(1)').get_text().strip(),
                'film_duration': film.select_one('div:nth-child(2) > div > div > div:nth-child(2) > span:nth-child(2)').get_text().strip(),
                'film_review': re.search(r'([0-9]+,?[0-9]+)', film.select_one('div:nth-child(2) > div > div > span > div > span').get_text().strip()).group(1).replace(',', '.'),
            }

            dataframe = pd.DataFrame([film_data])
            
            print(f'Film Name: {film_data["film_name"]} - Year Film: {film_data["year_film"]} - Film Duration: {film_data["film_duration"]} - Film Review: {film_data["film_review"]}')

            self.dataframe = pd.concat([self.dataframe, dataframe], ignore_index=True)

class ScraplingFilmesRequests:

    """
        Class that deals with the films scraping web scraping process using Selenium technology.

        Attributes
        ----------
            self.URL: str -> URL of the website to scraping;
            self.session: requests.Session -> requests session object to store the history of requests, cookies, etc;
            self.dataframe: pd.DataFrame -> Dataframe to store the scraped data;
            self.database: Database -> Database object to store the scraped data.

        Methods
        -------
            start -> Function that starts the scraping process;\n
            scraping_films -> Function that scrapes the names, year of release, duration and rating of each film;\n
            clean_database -> Function that cleans the database to insert new data;\n
            insert_data_to_database -> Function that inserts the data into the database.
    """

    def __init__(
        self: object
    ) -> None:

        self.URL = 'https://www.imdb.com'

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

    def start(self: object):

        """
            Function that starts the scraping process.
        """

        self.scraping_films()
        self.clean_database()
        self.insert_data_to_database()

    def scraping_films(self: object):

        """
            Function that scrapes the the names, year of release, duration and rating of each film.

            Exceptions
            ----------
                1. If it is not possible to collect the details of a movie, and within 3 attempts, an exception will be triggered.
        """

        tries = 0
        while True:
            if tries == 3:
                raise Exception(
                    f'After 3 attempts, it was not possible to collect the films.'
                )

            try:
                html_page = BeautifulSoup(
                    self.session.get(
                        self.URL + '/chart/top/?ref_=nv_mv_250',
                        timeout=60
                    ).content, 'html.parser')
                
                films = []
                for film in html_page.select('div.ipc-page-grid.ipc-page-grid--bias-left > div > ul > li'):
                    films.append(
                        {
                            'film_name': film.select_one('h3.ipc-title__text').get_text().split('.', 1)[-1].strip(),
                            'film_element': film
                        }
                    )
            
            except Exception as e:
                print(f'Attempt {tries} to collect the films failed. Error: {e}. Trying again...')
                tries += 1
                sleep(5)
            else:
                break

        for film in films:
            tries = 0
            while True:
                if tries == 3:
                    raise Exception(
                        f'After 3 attempts, we were unable to collect the details of the {film["film_name"]} film.'
                    )

                try:
                    film_data={
                        'film_name': film['film_name'],
                        'year_film': film['film_element'].select_one('div:nth-child(2) > div > div > div:nth-child(2) > span:nth-child(1)').get_text().strip(),
                        'film_duration': film['film_element'].select_one('div:nth-child(2) > div > div > div:nth-child(2) > span:nth-child(2)').get_text().strip(),
                        'film_review': re.search(r'([0-9]\.[0-9])', film['film_element'].select_one('div:nth-child(2) > div > div > span > div > span').get_text().strip()).group(1),
                    }
                    
                    dataframe = pd.DataFrame([film_data])

                    print(f'Film Name: {film["film_name"]} - Year Film: {film_data["year_film"]} - Film Duration: {film_data["film_duration"]} - Film Review: {film_data["film_review"]}')
                    
                    self.dataframe = pd.concat([self.dataframe, dataframe], ignore_index=True)

                except Exception as e:
                    print(f'Attempt {tries} to collect the details of the {film["film_name"]} film failed. Error: {e}. Trying again...')
                    tries += 1
                    sleep(5)
                else:
                    break

    def clean_database(self: object):

        """
            Function that cleans the database.
        """

        tries = 0
        while True:
            if tries == 3:
                raise Exception(
                    'After 3 attempts, it was not possible to clean the database.'
                )
            
            try:
                self.database.execute_query_to_clean_database(
                    'TRUNCATE TABLE mydatabase.films_scraping;'
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
                    data=self.dataframe, table_name='films_scraping', if_exists='append'
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
async def start_all_films():

    try:
        selenium_bot = ScraplingFilmesSelenium()
        selenium_bot.start()

    except Exception as e:
        print(f'Fail of the scraping process. Error: {e}.\n')
    else:
        print(f'Scraping process sucessfully.\n')
        selenium_bot.driver.quit()

    try:
        requests_bot = ScraplingFilmesRequests()
        requests_bot.start()

    except Exception as e:
        print(f'Fail of the scraping process. Error: {e}.\n')
    else:
        print(f'Scraping process sucessfully.\n')
 
    folder_directory = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    directory_report = folder_directory + os.sep + 'TMP' + os.sep + 'films_scraping'

    if not os.path.exists(directory_report):
        os.makedirs(directory_report)

    print(f'Saving the collected data in an excel report in the {Fore.GREEN}"{directory_report}"{Fore.RESET} directory.\n')
    with pd.ExcelWriter(directory_report + os.sep + 'films_scraping.xlsx') as writer:
        selenium_bot.dataframe.to_excel(writer, index=False, sheet_name='films_scraping_selenium')
        requests_bot.dataframe.to_excel(writer, index=False, sheet_name='films_scraping_requests')
    

loop = asyncio.get_event_loop()

try:
    loop.run_forever()

except Exception:
    loop.close()