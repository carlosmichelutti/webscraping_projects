from _load_env import db_user, db_pass, db_host, db_port, db_name
from utils_py.utils import Database

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

class AgriculturalProductsScraping:

    """
        Class that scrapes the agricultural products from the agrolink.com.br site.

        Attributes
        ----------
            self.URL: str -> URL of the website to scraping;
            self.session: requests.Session -> requests session object to store the history of requests, cookies, etc;
            self.dataframe: pd.DataFrame -> Dataframe to store the scraped data;
            self.database: Database -> Database object to store the scraped data;

        Methods
        -------
            start -> Function that starts the scraping process;
            extract_products -> Function that extracts the products;
            rename_columns_spreadsheet -> Function that renames the columns of the spreadsheet;
            clean_database -> Function that cleans the database to insert new data;
            insert_data_to_database -> Function that inserts the data into the database.
    """

    def __init__(
        self: object,
    ) -> None:

        self.URL = 'https://www.agrolink.com.br'

        self.session = requests.Session()
        self.session.headers.update(
            {
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
                'Accept-Encoding': 'gzip, deflate, br',
                'Accept-Language': 'pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7',
                'Content-Type': 'application/x-www-form-urlencoded',
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

        self.extract_products()
        self.rename_columns_spreadsheet()
        self.clean_database()
        self.insert_data_to_database()
    
    def extract_products(self: object):

        """
            Function that extracts the products.

            Exceptions
            ----------
                1. Exception - If it is not possible to collect the products from a page, and within 3 attempts, an exception will be raised.
        """

        page = 1
        while True:
            print(f'{Fore.GREEN}Moving to the page {page}...{Fore.RESET}')
            tries = 0
            while True:
                if tries == 3:
                    raise Exception(
                        f'After 3 attempts, it was not possible to collect the products from the page {page}.'
                    )

                try:
                    response = BeautifulSoup(
                        self.session.post(
                            url=self.URL + '/agrolinkfito/busca-direta-produto',
                            data={
                                'Cod_Registro': 0,
                                'NumeroPagina': page,
                                'TermoBuscaFito':  ' ',
                            }
                        ).content, 'html.parser')            

                    if response.select("div.block-section-main > div.row.row-blocks div.col-lg-4.col-xl-3.col-blocks"):
                        for row in response.select("div.block-section-main > div.row.row-blocks div.col-lg-4.col-xl-3.col-blocks"):
                            print(f'Colecting the product {row.select("a")[0].text.strip()}...')

                            line = pd.DataFrame(
                                [
                                    row.select('a')[0].text.strip(),
                                    row.select('a')[1].text.strip(),
                                    row.select('a')[2].text.strip()
                                ]
                            ).T

                            self.dataframe = pd.concat([self.dataframe, line])
                        
                        print('')
                        
                        page += 1

                    else:
                        print(f'{Fore.RED}There are no more products to collect.{Fore.RESET}')
                        return
                
                except Exception as e:
                    print(f'Attempt {tries} to collect the products from the page {page} failed. Error: {e}. Trying again...')
                    tries += 1
                    sleep(5)
                else:
                    break

    def rename_columns_spreadsheet(self: object):

        """
            Function that renames the columns of the spreadsheet.
        """

        self.dataframe.columns = ['product', 'company', 'active_ingredient']
    
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
                    'TRUNCATE TABLE mydatabase.agricultural_products_scraping;'
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
                    data=self.dataframe, table_name='agricultural_products_scraping', if_exists='append'
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
        bot = AgriculturalProductsScraping()
        bot.start()

    except Exception as e:
        print(f'Fail of the scraping process. Error: {e}.\n')
    else:
        print(f'Scraping process sucessfully.\n')
    finally:
        folder_directory = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        directory_report = folder_directory + os.sep + 'TMP' + os.sep + 'agricultural_products_scraping'

        if not os.path.exists(directory_report):
            os.makedirs(directory_report)

        print(f'Saving the collected data in an excel report in the {Fore.GREEN}"{directory_report}"{Fore.RESET} directory.\n')
        with pd.ExcelWriter(directory_report + os.sep + 'agricultural_products_scraping.xlsx') as writer:
            bot.dataframe.to_excel(writer, index=False, sheet_name='agricultural_products_scraping')


loop = asyncio.get_event_loop()

try:
    loop.run_forever()

except Exception:
    loop.close()