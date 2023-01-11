import lzma
import polars as pl
import numpy as np
from utils import Logger


def bytes_to_data(buffer: bytes) -> pl.DataFrame:
    """Convert a byte buffer to a polars DataFrame.

    Args:
        buffer: A byte buffer containing integer and float data.

    Returns:
        A polars DataFrame with columns "time", "ask", "bid", "ask_volume", and "bid_volume".
    """
    # Separate the buffer into integer and float columns
    integer_columns = np.frombuffer(buffer, dtype='>u4').reshape(-1, 5)[:, :3].astype(np.uint32)
    float_columns = np.frombuffer(buffer, dtype='>f4').reshape(-1, 5)[:, 3:].astype(np.float32)

    # Create a DataFrame from the integer and float columns
    df = pl.DataFrame({'time': integer_columns[:, 0], 'ask': integer_columns[:, 1], 'bid': integer_columns[:, 2],
                       'ask_volume': float_columns[:, 0], 'bid_volume': float_columns[:, 1]})
    return df


def standardize(day: pl.Date, ticks: pl.DataFrame) -> pl.DataFrame:
    """Standardize the prices in a polars DataFrame and create the date times.

    Args:
        day: The day of the data.
        ticks: A polars DataFrame with columns "time", "ask", and "bid".

    Returns:
        The input DataFrame with the "ask" and "bid" columns divided by 1000 and a correct datetime "time (UTC)" colum.
    """
    point = 1000  # Normalization factor
    ticks = ticks.with_columns(
        [(pl.duration(milliseconds="time") + pl.datetime(day.year, day.month, day.day)).alias("time (UTC)"),
         pl.col("ask") / point, pl.col("bid") / point])
    ticks.drop_in_place("time")
    return ticks


def decompress(day: pl.Date, compressed_buffer: bytes) -> pl.DataFrame:
    """Decompress a LZMA-compressed byte buffer and convert it to a polars DataFrame.

    Args:
        day: The day of the data.
        compressed_buffer: A LZMA-compressed byte buffer.

    Returns:
        A polars DataFrame with columns "time", "ask", "bid", "ask_volume", and "bid_volume". If the input buffer is empty,
        an empty DataFrame is returned.
    """
    # Check if the buffer is empty
    if not compressed_buffer:
        return pl.DataFrame(columns=["time", "ask", "bid", "ask_volume", "bid_volume"])

    # Attempt to decompress the buffer
    try:
        decompressed_buffer = lzma.decompress(compressed_buffer)
    except Exception as e:
        # Handle decompression errors
        Logger.error(f"Error decompressing buffer: {e}")
        print(f"Error decompressing buffer: {e}")
        return pl.DataFrame(columns=["time", "ask", "bid", "ask_volume", "bid_volume"])

    # Convert the decompressed
    return standardize(day, bytes_to_data(decompressed_buffer))
