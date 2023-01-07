#!/usr/bin/env python3.5

import argparse
from datetime import date, timedelta

from app import app
from utils import valid_date, set_up_signals,valid_timeframe, TimeFrame

VERSION = '0.2.1'


def main():
    parser = argparse.ArgumentParser(prog='data_downloader', usage='%(prog)s [options]')
    parser.add_argument('-v', '--version', action='version',
                        version='Version: %(prog)s-{version}'.format(version=VERSION))
    parser.add_argument('symbols', metavar='SYMBOLS', type=str, nargs='+',
                        help='symbol list using format EURUSD EURGBP')
    parser.add_argument('-d', '--day', type=valid_date, help='specific day format YYYY-MM-DD (default today)',
                        default=date.today() - timedelta(1))
    parser.add_argument('-s', '--startdate', type=valid_date, help='start date format YYYY-MM-DD (default today)')
    parser.add_argument('-e', '--enddate', type=valid_date, help='end date format YYYY-MM-DD (default today)')
    parser.add_argument('-t', '--thread', type=int, help='number of threads (default 20)', default=20)
    parser.add_argument('-f', '--folder', type=str, help='destination folder (default .)', default='.')
    parser.add_argument('-c', '--candle', type=valid_timeframe,
                        help='use candles instead of ticks. Accepted values M1 M2 M5 M10 M15 M30 H1 H4',
                        default=TimeFrame.TICK)
    parser.add_argument('--header', action='store_true', help='include CSV header (default True)', default=True)
    args = parser.parse_args()

    if args.startdate is not None:
        start = args.startdate
    else:
        start = args.day

    if args.enddate is not None:
        end = args.enddate
    else:
        end = args.day

    set_up_signals()
    app(args.symbols, start, end, args.thread, args.candle, args.folder, args.header)


if __name__ == '__main__':
    main()
