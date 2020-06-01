import aiohttp
import asyncio
import csv
import datetime
import io
import logging
import sqlite3
import sys
import time


start_time = time.time()
ENT = 42
OPT = 1
HTTP_CONCURRENCY = 4

console = logging.StreamHandler()
console.setLevel(logging.DEBUG)
console.setFormatter(logging.Formatter('%(levelname)-8s %(message)s'))

logger = logging.getLogger('ibdq')
logger.setLevel(logging.DEBUG)
logger.addHandler(console)


def read_securities(cur):
    cur.execute("SELECT zuniqueid, zsymbol FROM zsecurity WHERE LENGTH(zsymbol) <= 5")
    securities = cur.fetchall()
    logger.info(f'Found {len(securities)} securities...')

    return securities


async def get_stock_price(http, sec):
    security_id, symbol = sec
    logger.debug(f'Downloading prices for {symbol}...')
    url = f'https://query1.finance.yahoo.com/v7/finance/download/{symbol}?interval=1d&events=history'

    async with http.get(url) as response:
        if response.status == 200:
            price = next(csv.DictReader(io.StringIO(await response.text()), delimiter=','))
            price.update(
                {
                    'SecurityId': security_id,
                    'Symbol': symbol,
                    'Date': datetime.datetime.strptime(price['Date'], '%Y-%m-%d').date()
                }
            )

            return price


async def get_stock_prices(secs):
    connector = aiohttp.TCPConnector(keepalive_timeout=10, limit=HTTP_CONCURRENCY)
    async with aiohttp.ClientSession(connector=connector) as http:
        prices = await asyncio.gather(*[get_stock_price(http, sec) for sec in secs])
        return [price for price in prices if price]


def seconds_since_apple_epoch(date):
    return (date - datetime.date(2001, 1, 1) + datetime.timedelta(hours=12)).total_seconds()


def persist_stock_prices(cur, stock_prices):
    for stock_price in stock_prices:
        symbol = stock_price['Symbol']
        stock_price['SecondsSinceAppleEpoch'] = seconds_since_apple_epoch(stock_price['Date'])
        cur.execute(
            f'''UPDATE zprice 
            SET
                zvolume = :Volume,
                zclosingprice = :Close,
                zhighprice = :High,
                zlowprice = :Low,
                zopeningprice = :Open 
            WHERE
                z_ent = {ENT} AND z_opt = {OPT} AND 
                zdate = :SecondsSinceAppleEpoch AND zsecurityid = :SecurityId
            ''',
            stock_price
        )
        if cur.rowcount > 0:
            logger.debug(f'Existing entry for {symbol} updated...')
        else:
            cur.execute(
                f'''INSERT INTO zprice (
                    z_ent, z _opt, zdate, zsecurityid,
                    zvolume, zclosingprice, zhighprice, zlowprice, zopeningprice
                ) VALUES (
                    {ENT}, {OPT},
                    :SecondsSinceAppleEpoch,
                    :SecurityId,
                    :Volume,
                    :Close,
                    :High,
                    :Low,
                    :Open
                )
                ''',
                stock_price
            )
            logger.debug(f'New entry for {symbol} created...')

    cur.execute(
        '''UPDATE z_primarykey
        SET z_max = (SELECT MAX(z_pk) FROM zprice)
        WHERE z_name = 'Price'
        '''
    )
    logger.debug('Primary key for price updated...')
    logger.info(f'Persisted prices for {len(stock_prices)} securities...')


def sync_stock_prices(ibank_data_dir):
    sqlite_file = f'{ibank_data_dir}/accountsData.ibank'
    logger.info(f'Processing SQLite file {sqlite_file}...')
    with sqlite3.connect(sqlite_file) as conn:
        cur = conn.cursor()
        securities = read_securities(cur)
        stock_prices = asyncio.run(get_stock_prices(securities))
        persist_stock_prices(cur, stock_prices)


if __name__ == '__main__':
    if len(sys.argv) == 2:
        try:
            sync_stock_prices(sys.argv[1])
        except Exception as err:
            print('Error synchronizing stock prices', err)
            raise

        elapsed_time = time.time() - start_time
        logger.info(f'Security prices synchronized in {elapsed_time:.3f}s.')
    else:
        print("Please specify path to ibank data file.", file=sys.stderr)
        sys.exit(1)
