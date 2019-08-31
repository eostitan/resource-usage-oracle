## EOSIO Resource Usage Oracle

Python app which collates CPU/NET usage for all accounts interacting with the blockchain each day. It gathers from the get_blocks nodeos API, and submits to an EOSIO oracle contract after midnight once the previous days data has been received.

Data is stored in Redis which is persisted to file every 5 minutes.

All data is pruned to the most recent 7 days every hour.

A CSV file of the all data is exported to redis/accounts-usage.csv every 15 minutes.

Data submission uses a small node.js Express http server, as I'm not aware of an efficient Python library for pushing transactions.

### How to run

1) Install docker and docker-compose
2) Create the config.env file like the template, and modify as required
3) `docker-compose up -d`

### To monitor log file
`tail -f python_aggregator/debug.log`

...or for just errors...

`tail -f python_aggregator/debug.log | grep 'ERROR'`

### To delete retained redis data
1) `docker-compose down`
2) `rm redis/dump.rdb`

### LIMITATIONS
- Data submission is on a best efforts basis, confirmed only at the API endpoint. There are currently no checks to ensure it was added to an immutable block.