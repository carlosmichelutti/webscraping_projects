from _load_env import db_user, db_pass, db_host, db_port, db_name

from customized_package.utils import Database

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
import re


class BooksScraping:

    """
        Instance created to collect books separated by website categories: https://books.toscrape.com

        Attributes
        ----------
            self.URL: str -> URL of the website to scraping;
            self.session: requests.Session -> requests session object to store the history of requests, cookies, etc;
            self.dataframe: pd.DataFrame -> Dataframe to store the scraped data;
            self.database: Database -> Database object to store the scraped data;
            self.categories: list -> List with all categories.
        
        Methods
        -------
            start -> Function that starts the scraping process;\n
            get_all_categories -> Function that gets all categories;\n
            scraping_books -> Function that scrapes the books for each category;\n
            create_spreadsheet -> Function that creates the spreadsheet with the scraped data;\n
            clean_database -> Function that cleans the database to insert new data;\n
            insert_data_to_database -> Function that inserts the data into the database.
    """

    def __init__(
        self: object
    ) -> None:

        self.URL = 'https://books.toscrape.com'

        self.session = requests.Session()
        self.session.headers.update(
            {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
            }
        )

        self.dataframe = pd.DataFrame()

        self.database = Database(
            db_user=db_user,
            db_pass=db_pass,
            db_host=db_host,
            db_port=db_port,
            db_name=db_name
        )

        self.categories = []

    def start(self: object):

        """
            Function that starts the scraping process.
        """

        self.get_all_categories()
        for category in self.categories:
            self.scraping_books(category=category)
        
        self.create_spreadsheet()
        self.clean_database()
        self.insert_data_to_database()

    def get_all_categories(self: object):

        """
            Function that gets all categories.

            Exceptions
            ----------
                1. Exception - If it is not possible to collect all categories, and within 3 attempts, an exception will be raised.
        """

        tries = 0
        while True:
            if tries == 3:
                raise Exception(
                    'After 3 attempts, it was not possible to collect all categories.'
                )

            try:
                response = BeautifulSoup(
                    self.session.get(
                        url=self.URL + '/index.html',
                        timeout=30
                    ).content, 'html.parser')

                for category in response.select('div.side_categories > ul > li > ul > li > a'):
                    self.categories.append(
                        {
                            'category': category.text.strip() if not category.text.strip() == 'Books' else 'all_books',
                            'url': self.URL + '/' + category['href'],
                            'books': []
                        }
                    )

            except Exception as e:
                print(f'Attempt {tries} to collect all categories failed. Error: {e}. Trying again...')
                tries += 1
                sleep(5)
            else:
                break

    def scraping_books(self: object, category: dict):

        """
            Function that collects the books for each category.

            Parameters
            ----------
                category: dict -> Dictionary with the category data.

            Exceptions
            ----------
                1. Exception - If it is not possible to collect the books for this category, and within 3 attempts, an exception will be raised.
        """

        tries = 0
        while True:
            if tries == 3:
                raise Exception(
                    f'After 3 attempts, it was not possible to collect the books for this category {category["category"]}'
                )

            try:
                print(f'Collecting books from the {category["category"]} category.')

                response = BeautifulSoup(
                    self.session.get(
                        url=category['url'],
                        timeout=30
                    ).content, 'html.parser')

                books = []
                for book in response.select('article.product_pod'):
                    book = {
                        'book_title': book.select_one('h3 > a')['title'],
                        'book_element': book
                    }

                    books.append(book)

            except Exception as e:
                print(f'Attempt {tries} to collect books from the {category["category"]} category failed. Error: {e}. Trying again...')
                tries += 1
                sleep(5)
            else:
                break

        for book in books:
            tries = 0
            while True:
                if tries == 3:
                    raise Exception(
                        f'After {tries} attempts, it was not possible to collect the details of the book {book["book_title"]}.'
                    )

                try:
                    book = {
                        'category': category['category'],
                        'title':  book['book_title'],
                        'price': book['book_element'].select_one('p.price_color').text,
                        'book_details_url': self.URL + '/catalogue/' + re.search(r'../../../([a-zA-Z0-9-_]+/index.html)', book['book_element'].select_one('div.image_container > a')['href']).group(1)
                    }

                    category['books'].append(book)

                except Exception as e:
                    print(f'Attempt {tries} to collect the details of the book {book["book_title"]} failed. Error: {e}. Trying again...')
                    tries += 1
                    sleep(5)
                else:
                    break

    def create_spreadsheet(self: object):

        """
            Function that creates the spreadsheet with the scraped data.
        """
        for category in self.categories:
            self.dataframe = pd.concat([self.dataframe, pd.DataFrame(category['books'])], ignore_index=True)
        
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
                    'TRUNCATE TABLE mydatabase.books_scraping;'
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
                    data=self.dataframe, table_name='books_scraping', if_exists='append'
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
async def start_all_books():

    try:
        requests_bot = BooksScraping()
        requests_bot.start()

    except Exception as e:
        print(f'Fail of the scraping process. Error: {e}.')
    else:
        print(f'Scraping process sucessfully.\n')
    finally:
        folder_directory = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        directory_report = folder_directory + os.sep + 'TMP' + os.sep + 'books_scraping'

        if not os.path.exists(directory_report):
            os.makedirs(directory_report)

        print(f'Saving the collected data in an excel report in the {Fore.GREEN}"{directory_report}"{Fore.RESET} directory.\n')
        with pd.ExcelWriter(directory_report + os.sep + 'books_scraping.xlsx') as writer:
            for category in [{'category': 'all_books', 'books': []}] + requests_bot.categories:
                if not category['category'] == 'all_books':
                    dataframe = pd.DataFrame(category['books'])
                    dataframe.to_excel(writer, index=False, sheet_name=category['category'].capitalize())
                else:
                    dataframe = pd.DataFrame()
                    for category in requests_bot.categories:
                        dataframe = pd.concat([dataframe, pd.DataFrame(category['books'])], ignore_index=True)
                    dataframe.to_excel(writer, index=False, sheet_name='All books')

loop = asyncio.get_event_loop()

try:
    loop.run_forever()

except Exception:
    loop.close()