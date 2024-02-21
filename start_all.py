import os

path_projects = rf'C:\\Users\{os.getlogin()}\Documents\Webscraping_projects\\'
path_env = 'python'

def start_script(title: str, path_script: str, position_in_line: int = 0):
    """
        Function that returns the command to start the script in the new tab.

        Parameters
        ----------
            title: str - Title of the new tab;
            path_script: str - Path of the script;
            position_in_line: int - Queue position to run the script.
    """

    return f'''
    new-tab cmd.exe /k title {title} ^& {path_env} {
        os.path.join(path_projects, path_script)} {position_in_line}
    '''.strip()

scripts = [
    start_script('celphone_scraping', 'webscraping_projects\\celphone_scaping.py', 0), # webscraping cell phones with selenium and requests
    start_script('films_scraping', 'webscraping_projects\\films_scrping.py', 1), # webscraping films with selenium and requests
    start_script('books_scraping', 'webscraping_projects\\books_scraping.py', 2), # webscraping books with requests
    start_script('fuel_scraping', 'webscraping_projects\\fuel_scraping.py', 3), # webscraping fuel with requests
    start_script('products_agricultural_scraping', 'webscraping_projects\\products_agricultural_scraping.py', 4), # webscraping products agricultural with requests
    start_script('video_games_scraping', 'webscraping_projects\\videogames_scraping.py', 5), # webscraping video games with asyncio requests
]

os.system('wt --maximized ' + ' ; '.join(scripts))