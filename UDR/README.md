<img src="udr.png" height="300" />

This folder contains project that assist migration UDR processing from ORACLE to POSTGRES.

In this project, we imitate flow and logic from ORACLE (and php script) to processing UDR file.
Furhermore, we changed some php scripts to python scripts.

Quick review of udr flow as follow:
* 0- NMS Server send udr files to POSTGRES server (normally 7 files per minutes)
* 1- Received udr files is parsing by [parsing](./udr_parsing.py) script and inserted to *usa_trx_usage_data_records* (UDR) Postgres table (this run every minutes using cronjob scheduling)
* 2- Update *bb_rating.RTR_TRX_SUBSCRIBER_BALANCE* at MariaDB CBOSS using usage from UDR table, and flag the udr records as *STATE = 2* (every minutes, using cronjob scheduling)
* 3- After udr records has been flagged, move the records to *usa_trx_usage_data_records_log* (UDR LOG) by calling Postgres stored procedure, *bb_usage.p_usa_move_udr_to_udr_log()* (every 5 minutes scheduling using airflow)
* 4- Calculate udr usage using [calculation](./udr_log_calculation.py) script every 10 minutes (using cronjob scheduling)
* 5- Summary daily udr usage by calling Postgres stored procedure, *bb_usage.p_sum_daily_usage_caller()* (daily scheduling using airflow)


Beside that, every hour we have cronjob tasks that execute *php* scripts to copy tables from MariaDB CBOSS to Postgres. This tables are used with udr records to update *bb_rating.RTR_TRX_SUBSCRIBER_BALANCE* at MariaDB CBOSS

More details about parsing and calculation descriptions, is at [parsing](./parsing_readme.md) and [calculation](./calculation_readme.md).