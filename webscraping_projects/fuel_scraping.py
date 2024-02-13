from datetime import datetime, timedelta
from colorama import Fore
from time import sleep
import pandas as pd
import requests
import aiocron
import asyncio
import sys
import os
import io

class ScrapingFuel:



    def __init__(
        self: object
    ) -> None:

        self.URL = 'https://www.gov.br'

        self.session = requests.Session()
        self.session.headers.update(
            {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/89.0.4389.90 Safari/537.36',
            }
        )

        self.dataframe = pd.DataFrame()

    def start(self: object):

        """
            Function that starts the scraping process.
        """
        
        self.download_file()

    def download_file(self: object):

        """
            Function that downloads the file.

            Exceptions
            ----------
                1. Exception - If it is not possible to download the file, and within 3 attempts, an exception will be triggered.
        """

        tries = 0
        while True:
            if tries == 3:
                raise Exception(
                    'After 3 attempts, it was not possible to download the file.'
                )

            try:
                response = self.session.get(
                    self.URL + '/anp/pt-br/assuntos/precos-e-defesa-da-concorrencia/precos/precos-revenda-e-de-distribuicao-combustiveis/shlp/mensal/mensal_brasil-desde_jan2013.xlsx/@@download/file',
                    timeout=30
                )

                if response.status_code == 200 and type(response.content) == bytes:
                    print('The file was downloaded successfully.')
                    self.dataframe = pd.concat([self.dataframe, pd.read_excel(io.BytesIO(response.content), header=16)], ignore_index=True)
                else:
                    print(f'Attempt {tries} to download the file failed. Trying again...')
                    tries += 1
                    sleep(5)

            except Exception as e:
                print(f'Attempt {tries} to download the file failed. Error: {e}. Trying again...')
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
async def start_all_oils():

    try:
        bot = ScrapingFuel()
        bot.start()

    except Exception as e:
        print(f'Fail of the scraping process. Error: {e}.\n')
    else:
        print(f'Scraping process sucessfully.\n')
    finally:
        folder_directory = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        directory_report = folder_directory + os.sep + 'TMP' + os.sep + 'fuel_scraping'

        if not os.path.exists(directory_report):
            os.makedirs(directory_report)

        print(f'Saving the collected data in an excel report in the {Fore.GREEN}"{directory_report}"{Fore.RESET} directory.\n')
        with pd.ExcelWriter(directory_report + os.sep + 'fuel_scraping.xlsx') as writer:
            bot.dataframe.to_excel(writer, index=False, sheet_name='scraping_fuel')

loop = asyncio.get_event_loop()

try:
    loop.run_forever()

except Exception:
    loop.close()