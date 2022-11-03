from pydantic import BaseSettings
from dotenv import load_dotenv

class Config():
    def __init__(self):
        self.var1: str = load_dotenv('LOCAL_FILES_PATH')
        
conf = Config()
print(conf.var1)
# does not work