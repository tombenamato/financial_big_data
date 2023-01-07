import polars as pl
from utils import Logger

TEMPLATE_FILE_NAME = "{}-{}_{:02d}_{:02d}-{}_{:02d}_{:02d}.parquet"


class CSVFormatter(object):
    COLUMN_TIME = 0
    COLUMN_ASK = 1
    COLUMN_BID = 2
    COLUMN_ASK_VOLUME = 3
    COLUMN_BID_VOLUME = 4




class ParquetDumper:
    def __init__(self, symbol, timeframe, start, end, folder, header=True):
        self.symbol = symbol
        self.timeframe = timeframe
        self.start = start
        self.end = end
        self.folder = folder
        self.include_header = header
        self.buffer = {}

    def get_header(self):
        return ['time (UTC)', 'ask', 'bid', 'ask_volume', 'bid_volume']

    def append(self, day, ticks):
        if isinstance(ticks , pl.DataFrame):
            self.buffer[day] = ticks


    def dump(self):
        file_name = TEMPLATE_FILE_NAME.format(self.symbol,
                                              self.start.year, self.start.month, self.start.day,
                                              self.end.year, self.end.month, self.end.day)

        Logger.info("Writing {0}".format(file_name))

        df = pl.DataFrame( columns=[('ask', pl.Float64), ('bid', pl.Float64),
                                    ('ask_volume', pl.Float64), ('bid_volume', pl.Float64),
                                    ('time (UTC)', pl.Datetime)])
        for day in sorted(self.buffer.keys()):
            df.vstack(self.buffer[day], in_place=True)
        df = df.select(["time (UTC)", "ask", "bid","ask_volume", "bid_volume"])
        df.write_parquet(self.folder +"/" +file_name, compression="lz4")

        Logger.info("{0} completed".format(file_name))
