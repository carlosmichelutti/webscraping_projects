# from datetime import datetime, timedelta
# from bs4 import BeautifulSoup
# from time import sleep
# import pandas as pd
# import requests
# import os
# import io

# folder_directory = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
# directory_report = folder_directory + os.sep + 'TMP' + os.sep + 'world_population'

# if not os.path.exists(directory_report):
#     os.makedirs(directory_report)

# class WorldPopulation:

#     def __init__(
#         self: object,
#         init_date: datetime
#     ) -> None:
        
#         self.URL = 'https://www.worldometers.info'

#         self.session = requests.Session()
#         self.session.headers.update(
#             {
#                 'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.36',
#             }
#         )

#         self.init_date = init_date

#         self.dataframe = pd.DataFrame()

#     def start(self: object):
        
#         dataframe = self.scraping_data()
#         self.treat_data(dataframe=dataframe)

#     def scraping_data(self: object):
        
#         tries = 0
#         while True:
#             if tries == 3:
#                 raise Exception(
#                     'After 3 attempts, it was not possible to collect the data.'
#                 )
            
#             try:
#                 response = BeautifulSoup(
#                     self.session.get(
#                         url=self.URL + '/world-population/population-by-country',
#                         timeout=30
#                     ).content, 'html.parser')
                
#                 dataframe = pd.read_html(io.StringIO(response.select_one('table#example2').prettify()), thousands=',', decimal='.')[0]
#                 dataframe.insert(0, 'Day', self.init_date.date())
#                 dataframe['data_update'] = self.init_date

#                 print(f'Collect data from {self.init_date.strftime("%Y-%m-%d")}.')

#                 return dataframe

#             except Exception as e:
#                 print(f'Attempt {tries} to collect the data failed. Trying again...')
#                 tries += 1
#                 sleep(5)

#     def treat_data(self: object, dataframe: pd.DataFrame):

#         if self.dataframe.size:
#             self.dataframe = self.dataframe[self.dataframe['Day'] != pd.to_datetime(datetime.today().date())]

#         if self.dataframe.size:
#             self.dataframe = pd.concat([self.dataframe, dataframe], ignore_index=True)
#         else:
#             self.dataframe = dataframe


# bot = WorldPopulation(
#     init_date=datetime.today()
# )

# bot.start()

# bot.dataframe.to_excel(directory_report + os.sep + 'world_population.xlsx', index=False)