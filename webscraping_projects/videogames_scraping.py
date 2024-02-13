from _load_env import db_user, db_pass, db_host, db_port, db_name
from utils_py.utils import Database

from datetime import datetime, timedelta
from colorama import Fore
from time import sleep
import pandas as pd
import requests
import aiocron
import asyncio
import aiohttp
import json
import sys
import os

class VideoGamesScraping:

    def __init__(
        self: object
    ) -> None:
        
        self.URL = 'https://sandbox.oxylabs.io'

        self.session = requests.Session()

        self.dataframe = pd.DataFrame()

        self.database = Database(
            db_user=db_user,
            db_pass=db_pass,
            db_host=db_host,
            db_port=db_port,
            db_name=db_name
        )

        self.tasks = []
    
    async def start(self: object):

        """
            Function that starts the scraping process.
        """
        
        self.get_pages()
        await self.scrape_data()
        self.insert_data_to_database()

    def get_pages(self: object):

        """
            Function that obtains the number of pages to collect data.

            Exceptions
            ----------
                1. Exception - If it is not possible to get the number of pages, and within 3 attempts, an exception will be triggered.
        """
        
        tries = 0
        while True:
            if tries == 3:
                raise Exception(
                    'After 3 attempts, it was not possible to get the number of pages.'
                )
            
            try:
                self.pages = json.loads(
                    self.session.get(
                        url=self.URL + '/_next/data/-0UsSqtBWVEoFjJOOUmuF/products.json',
                        params={
                            'page': 1
                        },
                        timeout=30
                    ).content)['pageProps']['pageCount']
            
            except Exception as e:
                print(f'Attempt {tries} to get the number of pages failed. Error: {e}. Trying again...')
                tries += 1
                sleep(5)
            else:
                break

    async def scrape_data(self: object):

        """
            Function that scrapes the data from each page using asyncio requests.

            Exceptions
            ----------
                1. Exception - If it is not possible to get the data from a page, and within 5 attempts, an exception will be triggered.
        """

        async with aiohttp.ClientSession() as session:
            for page in range(1, self.pages + 1):
                print(f'Moving to page {page} of {self.pages} totals...')

                task = asyncio.ensure_future(
                    self.get_response(
                        session=session, page=page
                    )
                )

                self.tasks.append(task)           
                if len(self.tasks) == 10 or page == self.pages:
                    tries = 0
                    while True:
                        if tries == 5:
                            raise Exception(
                                f'After 5 attempts, it was not possible to get the data from the page {page}.'
                            )

                        results = await asyncio.gather(*self.tasks)
                        self.tasks.clear()

                        for result in results:
                            if isinstance(result, dict):
                                self.dataframe = pd.concat([self.dataframe, pd.DataFrame(result['pageProps']['products'])], ignore_index=True)
                            else:
                                task = asyncio.ensure_future(
                                    self.get_response(
                                        session=session, page=page
                                    )
                                )
                                self.tasks.append(task)  

                        if self.tasks:   
                            sleep(30 * 1 if not tries else 30 * tries)
                            tries += 1
                        else:
                            sleep(3)
                            break

    async def get_response(
            self: object,
            page: int,
            session: aiohttp.ClientSession, 
        ):

        """
            Function that obtains the data from a page.

            Parameters
            ----------
                page: int -> Page number to obtain the data.
                session: aiohttp.ClientSession -> Session to make the request asyncio.            
            Returns
            -------
                result_data: dict -> Data obtained from the page.
        """

        try:
            async with session.get(
                    url=self.URL + '/_next/data/-0UsSqtBWVEoFjJOOUmuF/products.json',
                    params={
                        'page': page
                    },
                    timeout=30
                ) as response:

                if response.status == 200:
                    result_data = await response.json()
                    return result_data
                else:
                    print(f"Error on page {page}: Status {response.status}")
                    return page
                
        except Exception as e:
            print(f"Error on page {page}. Error: {e}.")
            return page

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
                    data=self.dataframe, table_name='videogames_scraping', if_exists='append'
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
async def start_all_videogames():

    try:
        bot = VideoGamesScraping()

        await bot.start()

    except Exception as e:
        print(f'Fail of the scraping process. Error: {e}.')
    else:
        print(f'Scraping process sucessfully.\n')

    finally:
        folder_directory = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        directory_report = folder_directory + os.sep + 'TMP' + os.sep + 'videogames_scraping'

        if not os.path.exists(directory_report):
            os.makedirs(directory_report)
        
        print(f'Saving the collected data in an excel report in the {Fore.GREEN}"{directory_report}"{Fore.RESET} directory.\n')

        with pd.ExcelWriter(directory_report + os.sep + 'videogames_scraping.xlsx') as writer:
            bot.dataframe.to_excel(writer, index=False, sheet_name='videogames_scraping')

loop = asyncio.get_event_loop()

try:
    loop.run_forever()

except Exception:
    loop.close()