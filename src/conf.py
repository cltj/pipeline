from pydantic import BaseSettings

class Config(Basesettings):
    def __init__(self):
        self.var1
        
conf = Config()
print(conf.var)