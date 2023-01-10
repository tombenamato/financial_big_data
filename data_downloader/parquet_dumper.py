import polars as pl
from utils import Logger

TEMPLATE_FILE_NAME = "{}-{}_{:02d}_{:02d}-{}_{:02d}_{:02d}.parquet"


class ParquetDumper(object):
    def __init__(self, symbol: str, start: pl.Date, end: pl.Date, folder: str) -> None:
        """Initialize a new ParquetDumper instance.

        Args:
            symbol: The symbol to dump.
            start: The start date of the data.
            end: The end date of the data.
            folder: The folder to save the dumped data in.
        """
        self.symbol = symbol
        self.start = start
        self.end = end
        self.folder = folder

    def __enter__(self):
        self.buffer = {}
        return self

    def __exit__(self, *args):
        self.dump()
        self.buffer = {}

    def append(self, day: pl.Date, ticks: pl.DataFrame) -> None:
        """Append data for a specific day to the buffer.

        Args:
            day: The day of the data.
            ticks: The data to append.
        """
        if not ticks.is_empty():
            self.buffer[day] = ticks

    def dump(self) -> None:
        """Dump the data in the buffer to a Parquet file.

               The file will be saved in the folder specified in the constructor, with a name generated using the TEMPLATE_FILE_NAME
               template and the symbol, start date, and end date specified in the constructor.
        """
        file_name = TEMPLATE_FILE_NAME.format(self.symbol,
                                              self.start.year, self.start.month, self.start.day,
                                              self.end.year, self.end.month, self.end.day)

        Logger.info("Writing {0}".format(file_name))

        df = pl.DataFrame(columns=[('ask', pl.Float64), ('bid', pl.Float64),
                                   ('ask_volume', pl.Float32), ('bid_volume', pl.Float32),
                                   ('time (UTC)', pl.Datetime)])
        for day in sorted(self.buffer.keys()):
            df.vstack(self.buffer[day], in_place=True)
        df = df.select(["time (UTC)", "ask", "bid", "ask_volume", "bid_volume"])
        df.write_parquet(self.folder + "/" + file_name, compression="lz4")
        print(df)
        Logger.info("{0} completed".format(file_name))
