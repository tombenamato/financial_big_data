import struct
from lzma import LZMADecompressor, LZMAError, FORMAT_AUTO
import polars as pl

def decompress_lzma(data):
    results = []
    len(data)
    while True:
        decomp = LZMADecompressor(FORMAT_AUTO, None, None)
        try:
            res = decomp.decompress(data)
        except LZMAError:
            if results:
                break  # Leftover data is not a valid LZMA/XZ stream; ignore it.
            else:
                raise  # Error on the first iteration; bail out.
        results.append(res)
        data = decomp.unused_data
        if not data:
            break
        if not decomp.eof:
            raise LZMAError("Compressed data ended before the end-of-stream marker was reached")
    return b"".join(results)


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
    #return df.to_dict(as_series=False)
    #def norm(time, ask, bid, volume_ask, volume_bid):
    #    date = datetime(day.year, day.month, day.day) + timedelta(milliseconds=time)
    #    #date.replace(tzinfo=datetime.tzinfo("UTC"))
    #    return date, ask / point, bid / point, volume_ask , volume_bid
    #return map(lambda x: norm(*x), ticks)


def decompress(symbol, day, compressed_buffer):
    if compressed_buffer.nbytes == 0:
        return compressed_buffer
    return normalize(symbol, day, tokenize(decompress_lzma(compressed_buffer)))
