from datetime import datetime, timedelta
from time import sleep, time
from colorama import Fore
import pandas as pd
import requests
import aiocron
import asyncio
import json
import sys
import os

class AgriculturalProductsScraping:

    session: requests.Session = requests.Session()
    session.headers.update(
        {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/133.0.0.0 Safari/537.36'
        }
    )

    def __init__(
        self: object
    ) -> None:

        self.URL: str = 'https://www.agrolink.com.br'

        self.products: list = []

    def start(self: object) -> None:

        self.scraping_products()

    def scraping_products(self: object) -> None:

        attempts = 0
        while True:
            if attempts == 3:
                raise Exception(
                    'After 3 attempts, it was not possible to collect the products.'
                )

            try:
                response = self.session.post(
                    url=self.URL + '/agrolinkfito/ListaProdutosBusca',
                    timeout=90
                )

                if response.status_code == 200:
                    response = json.loads(response.content)
                    [self.products.append(product) for product in response['ViewModel']['produtos']]
                    break

                print(f'Attempt {attempts} to collect the products failed. Response: {response}. Trying again...')
                attempts += 1
                sleep(10)

            except Exception as e:
                print(f'Attempt {attempts} to collect the products failed. Error: {e}. Trying again...')
                attempts += 1
                sleep(10)

additional_minutes = 1 + int(sys.argv[1]) if len(sys.argv) > 1 else 1
if datetime.now().second >= 50:
    additional_minutes += 1
cron = (datetime.now() + timedelta(minutes=additional_minutes)).strftime('%M %H * * *')
print(f'This script will start at {datetime.strptime(cron, "%M %H * * *").strftime("%H:%M")}.')

@aiocron.crontab(cron, start=True)
async def start_scraping_agricultural_products_initial():

    init_time = time()

    try:
        bot = AgriculturalProductsScraping()

        bot.start()

    except Exception as e:
        print(f'Fail of the scraping process. Error: {e}.')

    else:
        print(f'Scraping process successfully completed in {Fore.GREEN}{time() - init_time}{Fore.RESET} seconds.')

    finally:
        directory_report = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'TMP', 'agricultural_products_scraping')
        os.makedirs(directory_report, exist_ok=True)

        print(f'Saving the collected data in an excel report in the {Fore.GREEN}"{directory_report}"{Fore.RESET} directory.')
        with pd.ExcelWriter(os.path.join(directory_report, 'agricultural_products_scraping.xlsx')) as writer:
            dataframe = pd.DataFrame(bot.products)
            dataframe['created_at'] = datetime.now()
            dataframe.to_excel(writer, index=False, sheet_name='agricultural_products_scraping')

        start_scraping_agricultural_products_initial.stop()

@aiocron.crontab('0 8 * * *', start=True)
async def start_scraping_agricultural_products_recursively():

    init_time = time()

    try:
        bot = AgriculturalProductsScraping()

        bot.start()

    except Exception as e:
        print(f'Fail of the scraping process. Error: {e}.')

    else:
        print(f'Scraping process successfully completed in {Fore.GREEN}{time() - init_time}{Fore.RESET} seconds.')

    finally:
        directory_report = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'TMP', 'agricultural_products_scraping')
        os.makedirs(directory_report, exist_ok=True)

        print(f'Saving the collected data in an excel report in the {Fore.GREEN}"{directory_report}"{Fore.RESET} directory.')
        with pd.ExcelWriter(os.path.join(directory_report, 'agricultural_products_scraping.xlsx')) as writer:
            dataframe = pd.DataFrame(bot.products)
            dataframe['created_at'] = datetime.now()
            dataframe.to_excel(writer, index=False, sheet_name='agricultural_products_scraping')

loop = asyncio.get_event_loop()

try:
    loop.run_forever()

except Exception:
    loop.close()
