# core imports
import logging
import traceback
import json
import os
import time
import signal
from datetime import datetime, timedelta, date

# external library imports
import requests
import redis

# app level imports
from utils import seconds_to_time_string, get_contract_configuration_state, get_contract_expected_dataset_id

# get environment variables
PUSH_API_NODE = os.getenv('EOSIO_PUSH_API_NODE', '')
CONTRACT_ACCOUNT = os.getenv('CONTRACT_ACCOUNT', '')
CONTRACT_ACTION = os.getenv('CONTRACT_ACTION', '')
SUBMISSION_ACCOUNT = os.getenv('SUBMISSION_ACCOUNT', '')
SUBMISSION_PERMISSION = os.getenv('SUBMISSION_PERMISSION', '')
SUBMISSION_INTERVAL_SECONDS = int(os.getenv('SUBMISSION_INTERVAL_SECONDS', 10))

# for gracefully handling docker signals
KEEP_RUNNING = True
def stop_container(*args):
    global KEEP_RUNNING
    KEEP_RUNNING = False
signal.signal(signal.SIGTERM, stop_container)

# logging configuration
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
handler = logging.FileHandler('debug.log')
handler.setLevel(logging.INFO)
formatter = logging.Formatter(f'%(asctime)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)

# establish connection to redis server
redis = redis.StrictRedis(host='redis', port=6379, db=0, decode_responses=True)

# wait for redis dump file to signify that DB is ready
while KEEP_RUNNING and not os.path.exists('/data/dump.rdb'):
    time.sleep(1)

# check for complete periods of data and submit at the appropriate time in contract lifecycle
while KEEP_RUNNING:
    current_time_seconds = datetime.now().timestamp()

    # prune old redis data and get most recent submission data available
    most_recent_submission_data_seconds = 0
    for key in sorted(redis.keys('SUBMISSION_DATA_*')):
        pss = int(key[16:])
        if current_time_seconds > pss + (3600 *24 * 28): # more than 28 days ago
            redis.delete(key)
        most_recent_submission_data_seconds = pss
    logger.info(f'Most recent data available to submit: {seconds_to_time_string(most_recent_submission_data_seconds)}')

    # get current data submission period from contract
    period_start_seconds, period_seconds, _, state = get_contract_configuration_state()

    # if collection interval for contract period start has passed, call nextperiod to advance it
    if most_recent_submission_data_seconds > period_start_seconds: # + (period_seconds * 2):
        action = {
            "account": CONTRACT_ACCOUNT,
            "name": "nextperiod",
            "authorization": [{
                "actor": SUBMISSION_ACCOUNT,
                "permission": SUBMISSION_PERMISSION,
            }],
            "data": {}
        }
        logger.info(f'Calling nextperiod contract action, to advance submission period...')
        tx = {'actions': [action]}
        logger.info(tx)
        response = requests.post('http://eosjs_server:3000/push_transaction', json=tx, timeout=10).json()
        if 'transaction_id' in response:
            logger.info(f'Transaction {response["transaction_id"]} successfully submitted!')
            time.sleep(1)
            continue # if advanced the period, then continue to next perioo before submitting data
        else:
            logger.info(f'Could not advance submission period...')
            if 'error' in response:
                logger.info(response['error'])

    if period_start_seconds:
        period_start_seconds = int(period_start_seconds)
        logger.info(f'Current submission period is {seconds_to_time_string(period_start_seconds)}')

        # submit data
        data = redis.get('SUBMISSION_DATA_' + str(period_start_seconds))
        if data:
            data = json.loads(data)
            dataset_id = get_contract_expected_dataset_id()
            if dataset_id == 0 and state == 'TOTAL_USAGE': # send totals
                action = {
                    "account": CONTRACT_ACCOUNT,
                    "name": "settotalusg",
                    "authorization": [{
                        "actor": SUBMISSION_ACCOUNT,
                        "permission": SUBMISSION_PERMISSION,
                    }],
                    "data": {"source": SUBMISSION_ACCOUNT,
                        "total_cpu_us": data['total_cpu_usage_us'],
                        "total_net_words": data['total_net_usage_words'],
                        "all_data_hash": data['all_data_hash'],
                        "period_start": datetime.fromtimestamp(period_start_seconds).strftime('%Y-%m-%dT%H:%M:%S')
                    }
                }
                logger.info(f'Submitting resource usage totals for {seconds_to_time_string(period_start_seconds)}...')
                tx = {'actions': [action]}
                logger.info(tx)
                response = requests.post('http://eosjs_server:3000/push_transaction', json=tx, timeout=10).json()
                if 'transaction_id' in response:
                    logger.info(f'Transaction {response["transaction_id"]} successfully submitted!')
                else:
                    logger.info(f'Transaction could not be submitted!')
                    if 'error' in response:
                        logger.info(response['error'])

            elif dataset_id > 0 and dataset_id < len(data['usage_datasets']) and state == 'INDIVIDUAL_USAGE': # send individual accounts dataset
                dataset = data['usage_datasets'][dataset_id]
                action = {
                    "account": CONTRACT_ACCOUNT,
                    "name": "addactusg",
                    "authorization": [{
                        "actor": SUBMISSION_ACCOUNT,
                        "permission": SUBMISSION_PERMISSION,
                    }],
                    "data": {"source": SUBMISSION_ACCOUNT,
                        "dataset_id": dataset_id,
                        "dataset": dataset,
                        "period_start": datetime.fromtimestamp(period_start_seconds).strftime('%Y-%m-%dT%H:%M:%S')
                    }
                }
                logger.info(f'Submitting accounts resource usage for {seconds_to_time_string(period_start_seconds)}...')
                tx = {'actions': [action]}
                logger.info(tx)
                response = requests.post('http://eosjs_server:3000/push_transaction', json=tx, timeout=60).json()
                if 'transaction_id' in response:
                    logger.info(f'Transaction {response["transaction_id"]} successfully submitted!')
                else:
                    logger.info(f'Transaction could not be submitted!')
                    if 'error' in response:
                        logger.info(response['error'])

            elif dataset_id == len(data['usage_datasets']): # all sent
                logger.info(f'All data submitted for {seconds_to_time_string(period_start_seconds)}')

            elif state == 'TOTAL_USAGE':
                logger.info(f'Awaiting inflation transfer for {seconds_to_time_string(period_start_seconds)}')

    time.sleep(SUBMISSION_INTERVAL_SECONDS)