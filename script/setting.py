from dotenv import load_dotenv
from pathlib import Path

env_path = Path('../.env')
print ("[INFO] - Define .env file path :" + str(env_path.absolute()))
load_dotenv(dotenv_path=env_path)
