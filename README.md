## EOSIO Resource Usage Oracle

Python app which collates CPU/NET usage for all accounts interacting with the blockchain each day. It gathers from the get_blocks nodeos API in the `aggregator` process, and submits to an EOSIO oracle contract after the period has ended in the `submitter` process.

It stays aware of the state of the contract and sends the total system CPU/NET usage for the period, and then the individual account CPU usage totals in several actions as determined by the contract. It resubmits any data that doesn't make it into an irreversible block.

Data is stored in Redis which is persisted to file every 5 minutes.

All data is pruned to the most recent 28 days worth.

Data submission uses a small node.js Express http server.

![Data Flow Diagram](data-flow.png)

### How to run

1) Install docker and docker-compose
2) Create the config.env file like the template, and modify as required
3) `docker-compose up -d`

### How to stop
`docker-compose down`

### To monitor log file
`tail -f python/debug.log`

### To delete retained redis data
1) `docker-compose down`
2) `rm redis/dump.rdb`

### Running multiple oracle instances on one machine (for testing)
1) Copy whole repo to a new directory for each oracle
2) Update config.env in these directories to reflect appropriate submission credentials for each oracle
3) `docker-compose up -d` in each directory

### Testing mode
If you want to use standard totals and usage data, and for ease of testing the contract, you can edit config.env to uncomment TEST_USAGE_DATA line before starting, and the [aggregate_test_data.py](python/src/aggregate_test_data.py) file can be edited.

### Low priority TODOs
- Prevent contract reconfiguration from requiring existing data to be deleted manually
- Replace node.js express server with https://github.com/EOSArgentina/ueosio library