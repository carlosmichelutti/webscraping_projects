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

class FilmsScraping:

    """
        Class that scrapes all the films from IMDb.

        Attributes
        ----------
        session: requests.Session
            A request session object used to store cookies and request history.

        URL: str
            The URL of the website to scrape.

        films: list
            A list that stores all scraped films.

        Methods
        -------
        __init__()
            Constructor that initializes the necessary variables.

        start()
            Function responsible for controlling the scraping process.

        scraping_films()
            Function that collects all films from the IMDb top chart.
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

        self.URL: str = 'https://www.imdb.com'

        self.films: list = []

    def start(self: object) -> None:

        """
            Function responsible for controlling the scraping process.
        """

        self.scraping_films()

    def scraping_films(self: object) -> None:

        """
            Function that collects all films from the IMDb top chart.
        """

        attempts = 0
        while True:
            if attempts == 3:
                raise Exception(
                    'After 3 attempts, it was not possible to collect the films.'
                )

            try:
                response = self.session.get(
                    url=self.URL + '/chart/top',
                    params={
                        'ref_': 'nv_mv_250'
                    },
                    timeout=90
                )

                if response.status_code == 200:
                    response = BeautifulSoup(response.content, 'lxml', from_encoding='utf-8')
                    films = response.select('li.ipc-metadata-list-summary-item')
                    for index, film in enumerate(films):
                        print(f'Collecting film Name: {film.select_one("h3.ipc-title__text").text}. Film {index+1} of the {len(films)} films.')
                        self.films.append(
                            {
                                'film_name': film.select_one('h3.ipc-title__text').text.split('.', 1)[-1].strip(),
                                'film_year': film.select_one('div[class*="title-metadata"] > span:nth-child(1)').text.strip(),
                                'film_duration': film.select_one('div[class*="title-metadata"] > span:nth-child(2)').text.strip(),
                                'film_review': re.search(r'([0-9]\.[0-9])', film.select_one('span.ipc-rating-star--rating').text.strip()).group(1)
                            }
                        )
                    break

                print(f'Attempt {attempts} to collect the films failed. Response: {response}. Trying again...')
                attempts += 1
                sleep(10)

            except Exception as e:
                print(f'Attempt {attempts} to collect the films failed. Error: {e}. Trying again...')
                attempts += 1
                sleep(10)

additional_minutes = 1 + int(sys.argv[1]) if len(sys.argv) > 1 else 1
if datetime.now().second >= 50:
    additional_minutes += 1
cron = (datetime.now() + timedelta(minutes=additional_minutes)).strftime('%M %H * * *')
print(f'This script will start at {datetime.strptime(cron, "%M %H * * *").strftime("%H:%M")}.')

@aiocron.crontab(cron, start=True)
async def start_scraping_films_initial():

    init_time = time()

    try:
        bot = FilmsScraping()

        bot.start()

    except Exception as e:
        print(f'Fail of the scraping process. Error: {e}.')

    else:
        print(f'Scraping process successfully completed in {Fore.GREEN}{time() - init_time}{Fore.RESET} seconds.')

    finally:
        directory_report = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'TMP', 'films_scraping')
        os.makedirs(directory_report, exist_ok=True)

        print(f'Saving the collected data in an excel report in the {Fore.GREEN}"{directory_report}"{Fore.RESET} directory.')
        with pd.ExcelWriter(os.path.join(directory_report, 'films_scraping.xlsx')) as writer:
            dataframe = pd.DataFrame(bot.films)
            dataframe.to_excel(writer, index=False, sheet_name='films')

        start_scraping_films_initial.stop()

@aiocron.crontab('0 8 * * *', start=True)
async def start_scraping_films_recursively():

    init_time = time()

    try:
        bot = FilmsScraping()

        bot.start()

    except Exception as e:
        print(f'Fail of the scraping process. Error: {e}.')

    else:
        print(f'Scraping process successfully completed in {Fore.GREEN}{time() - init_time}{Fore.RESET} seconds.')

    finally:
        directory_report = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'TMP', 'films_scraping')
        os.makedirs(directory_report, exist_ok=True)

        print(f'Saving the collected data in an excel report in the {Fore.GREEN}"{directory_report}"{Fore.RESET} directory.')
        with pd.ExcelWriter(os.path.join(directory_report, 'films_scraping.xlsx')) as writer:
            dataframe = pd.DataFrame(bot.films)
            dataframe.to_excel(writer, index=False, sheet_name='films')

loop = asyncio.get_event_loop()

try:
    loop.run_forever()

except Exception:
    loop.close()
