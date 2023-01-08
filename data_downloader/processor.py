import lzma
import struct
from lzma import LZMADecompressor, LZMAError, FORMAT_AUTO
import polars as pl
import numpy as np


def tokenize(buffer):
    tokens = struct.iter_unpack('>IIIff', buffer)
    return tokens

def normalize(symbol, day, ticks):
    point = 1000
    df = pl.DataFrame(ticks, columns=['time', 'ask', 'bid', 'ask_volume', 'bid_volume'])
    df = df.with_columns([(pl.duration(milliseconds="time")+pl.datetime(day.year, day.month, day.day)).alias("time (UTC)")
                             ,pl.col("ask")/point, pl.col("bid")/point])
    df.drop_in_place("time")
    return df


def decompress(symbol, day, compressed_buffer):
    if compressed_buffer.nbytes == 0:
        return compressed_buffer
    return normalize(symbol, day, tokenize(lzma.decompress(compressed_buffer)))
