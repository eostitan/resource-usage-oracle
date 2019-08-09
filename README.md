## EOSIO Resource Usage Aggregator

Python app which collects CPU/NET usage for all transactions using the get_blocks API, and submits to an EOSIO oracle contract.

Data submission uses a small node.js Express http server, as I'm not aware of an efficient Python library for pushing transactions.

### How to Run

1) Install docker and docker-compose
2) Create the config.env file like the template, and modify as required
3) `docker-compose up -d`

### To monitor log file
`tail -f python_aggregator/debug.log`