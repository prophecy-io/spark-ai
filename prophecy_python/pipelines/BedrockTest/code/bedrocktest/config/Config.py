from prophecy.config import ConfigBase


class Config(ConfigBase):

    def __init__(self, **kwargs):
        self.spark = None
        self.update()

    def update(self, **kwargs):
        prophecy_spark = self.spark
        pass
