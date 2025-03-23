import os

path_projects = rf'C:\\Users\{os.getlogin()}\Documents\GitHub\Webscraping_projects\\'
path_env = 'python'

def start_script(title: str, path_script: str, position_in_line: int = 0):

    return f'new-tab cmd.exe /k title {title} ^& {path_env} {os.path.join(path_projects, path_script)} {position_in_line}'

scripts = [
    start_script('Books Scraping', 'webscraping_projects\\books_scraping.py', 0), # Books scraper.
    start_script('Films Scraping', 'webscraping_projects\\films_scraping.py', 1), # Films scraper.
    start_script('Products Scraping', 'webscraping_projects\\products_scraping.py', 2), # Products scraper.
]

os.system('wt --maximized ' + ' ; '.join(scripts))
