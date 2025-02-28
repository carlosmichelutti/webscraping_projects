from datetime import datetime, timedelta
from bs4 import BeautifulSoup
from time import sleep, time
from colorama import Fore
import pandas as pd
import requests
import aiocron
import asyncio
import sys
import os
import re

class BooksScraping:

    """
        Class that scrapes all the books.

        Attributes
        ----------
        session: requests.Session
            A request session object used to store cookies and request history.

        URL: str
            The URL of the website to scrape.

        categories: list
            A list that stores all available categories.

        books: list
            A list that stores all scraped books.

        Methods
        -------
        __init__()
            Constructor that initializes the necessary variables.

        start()
            Function responsible for controlling the scraping process.

        get_categories()
            Function that collects all available categories.

        scraping_books()
            Function that collects all books from all categories.
    """

    session: requests.Session = requests.Session()
    session.headers.update(
        {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/133.0.0.0 Safari/537.36'
        }
    )

    def __init__(
        self: object
    ) -> None:

        """
            Constructor that initializes the necessary variables.
        """

        self.URL: str = 'https://books.toscrape.com'

        self.categories: list = []

        self.books: list = []

    def start(self: object) -> None:

        """
            Function responsible for controlling the scraping process.
        """

        self.get_categories()
        self.scraping_books()

    def get_categories(self: object) -> None:

        """
            Function that collects all available categories.
        """

        attempts = 0
        while True:
            if attempts == 3:
                raise Exception(
                    'After 3 failed attempts, it was not possible to collect all categories.'
                )

            try:
                response = self.session.get(
                    url=self.URL + '/index.html',
                    timeout=90
                )

                if response.status_code == 200:
                    response = BeautifulSoup(response.content, 'html.parser')
                    for category in response.select('div.side_categories ul:nth-child(2) a[href*="catalogue/category/books"]'):
                        self.categories.append(
                            {
                                'category': category.text.strip(),
                                'url': (self.URL + '/' + category['href']).replace('/index.html', '').strip(),
                                'books': []
                            }
                        )
                    break

                print(f'Attempt {attempts} to collect all categories failed. Response: {response}. Trying again...')
                attempts += 1
                sleep(10)

            except Exception as e:
                print(f'Attempt {attempts} to collect all categories failed. Error: {e}. Trying again...')
                attempts += 1
                sleep(10)

    def scraping_books(self: object) -> None:

        """
            Function that collects all books from all categories.
        """

        for index, category in enumerate(self.categories):
            print(f'Collecting books from the {category["category"]} category. {index + 1} of {len(self.categories)} categories...')
            attempts = 0
            while True:
                if attempts == 3:
                    raise Exception(
                        f'After 3 failed attempts, it was not possible to collect the number of pages for this category {category["category"]}.'
                    )

                try:
                    response = self.session.get(
                        url=category['url'] + '/index.html',
                        timeout=90
                    )

                    if response.status_code == 200:
                        response = BeautifulSoup(response.content, 'html.parser')
                        quantity_items = response.select_one('form.form-horizontal strong:nth-child(2)')
                        if not quantity_items or int(quantity_items.text) == 0:
                            self.pages = 0
                        elif int(quantity_items.text) % 20 == 0:
                            self.pages = int(quantity_items.text) // 20
                        else:
                            self.pages = int(quantity_items.text) // 20 + 1
                        break

                    print(f'Attempt {attempts} to collect the number of pages for this category {category["category"]} failed. Response: {response}. Trying again...')
                    attempts += 1
                    sleep(10)

                except Exception as e:
                    print(f'Attempt {attempts} to collect the number of pages for this category {category["category"]} failed. Error: {e}. Trying again...')
                    attempts += 1
                    sleep(10)

            for page in range(1, self.pages + 1):
                attempts = 0
                while True:
                    if attempts == 3:
                        raise Exception(
                            f'After 3 failed attempts, it was not possible to collect books from the category {category["category"]} and page {page}.'
                        )

                    try:
                        response = self.session.get(
                            url=category['url'] + '/index.html' if page == 1 else category['url'] + f'/page-{page}.html',
                            timeout=90
                        )

                        if response.status_code == 200:
                            response = BeautifulSoup(response.content, 'html.parser')
                            for book in response.select('article.product_pod'):
                                self.books.append(
                                    {
                                        'category': category['category'],
                                        'book_title': book.select_one('h3 a')['title'],
                                        'book_price': float(re.sub('[^\d.]', '', book.select_one('div.product_price p.price_color').text)),
                                        'book_image': self.URL + '/media' + book.select_one('div.image_container img')['src'].split('/media')[-1],
                                        'book_rating': ''
                                    }
                                )
                            break

                        print(f'Attempt {attempts} to collect books from the category {category["category"]} and page {page} failed. Response: {response}. Trying again...')
                        attempts += 1
                        sleep(10)

                    except Exception as e:
                        print(f'Attempt {attempts} to collect books from the category {category["category"]} and page {page} failed. Error: {e}. Trying again...')
                        attempts += 1
                        sleep(10)

additional_minutes = 1 + int(sys.argv[1]) if len(sys.argv) > 1 else 1
if datetime.now().second >= 50:
    additional_minutes += 1
cron = (datetime.now() + timedelta(minutes=additional_minutes)).strftime('%M %H * * *')
print(f'This script will start at {datetime.strptime(cron, "%M %H * * *").strftime("%H:%M")}.')

@aiocron.crontab(cron, start=True)
async def start_scraping_books_initial():

    init_time = time()

    try:
        bot = BooksScraping()

        bot.start()

    except Exception as e:
        print(f'Fail of the scraping process. Error: {e}.')

    else:
        print(f'Scraping process successfully completed in {Fore.GREEN}{time() - init_time}{Fore.RESET} seconds.')

    finally:
        directory_report = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'TMP', 'books_scraping')
        os.makedirs(directory_report, exist_ok=True)

        print(f'Saving the collected data in an excel report in the {Fore.GREEN}"{directory_report}"{Fore.RESET} directory.')
        with pd.ExcelWriter(os.path.join(directory_report, 'books_scraping.xlsx')) as writer:
            dataframe = pd.DataFrame(bot.books)
            dataframe['created_at'] = datetime.now()
            dataframe.to_excel(writer, index=False, sheet_name='books')

        start_scraping_books_initial.stop()

@aiocron.crontab('0 8 * * *', start=True)
async def start_scraping_books_recursively():

    init_time = time()

    try:
        bot = BooksScraping()

        bot.start()

    except Exception as e:
        print(f'Fail of the scraping process. Error: {e}.')

    else:
        print(f'Scraping process successfully completed in {Fore.GREEN}{time() - init_time}{Fore.RESET} seconds.')

    finally:
        directory_report = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'TMP', 'books_scraping')
        os.makedirs(directory_report, exist_ok=True)

        print(f'Saving the collected data in an excel report in the {Fore.GREEN}"{directory_report}"{Fore.RESET} directory.')
        with pd.ExcelWriter(os.path.join(directory_report, 'books_scraping.xlsx')) as writer:
            dataframe = pd.DataFrame(bot.books)
            dataframe['created_at'] = datetime.now()
            dataframe.to_excel(writer, index=False, sheet_name='books')

loop = asyncio.get_event_loop()

try:
    loop.run_forever()

except Exception:
    loop.close()
