## EOSIO Resource Usage Aggregator

Python app which collects CPU/NET usage for all transactions using the get_blocks API, and submits to an EOSIO oracle contract.

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

### To delete retained redis data
1) `docker-compose down`
2) `rm redis/dump.rdb`

### TODO
- Ensure this submits ok to real contract when available
- Organise submission schedule so that previous days data is sent 1 hour after it ends, and spread out to ensure a low tx rate
- Add checks to ensure all data made it into immutable blocks?
- Stress testing by using contract on EOS mainnet?