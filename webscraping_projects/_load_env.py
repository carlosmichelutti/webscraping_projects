from dotenv import load_dotenv
import os

load_dotenv()

# database connection parameters
db_user = os.getenv('db_user')
db_pass = os.getenv('db_pass')
db_host = os.getenv('db_host')
db_port = os.getenv('db_port')
db_name = os.getenv('db_name')