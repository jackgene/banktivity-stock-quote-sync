# Banktivity Stock Quote Synchronizer
Banktivity stock quote synchronization tool in various languages.

The purpose of this simple application is to evaluate the implementation of
some basic functionality in a variety of programming languages.

Every implementation should perform the following:
- Take the path to the .ibank data dir from the first command line argument.
- Open accountsData.ibank within directory as a SQLite database.
- Read stock symbols from the `ZSECURITY` table.
- Make an HTTP request to download stock prices from sparc-service.
- Parse response as JSON.
- Update the following SQLite tables in a single transaction:
  - `UPDATE`/`INSERT` stock prices in the `ZPRICE`.
  - `UPDATE` prices primary key in the `Z_PRIMARYKEY` table.
- Log operations in a consistent manner, using each platforms standard logging.

Ideas to consider:
- Use the platform's argument parser to parse arguments.
- Use the platform's standard configuration format for configurations.

Swift implementation is the reference implementation for:
- Log levels and messages

