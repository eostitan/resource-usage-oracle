import logging
import traceback
import json
import os
import time
import signal
from datetime import datetime, timedelta, date
from concurrent.futures import ThreadPoolExecutor
import requests
import redis

# get environment variables
PUSH_API_NODE = os.getenv('EOSIO_PUSH_API_NODE', '')
CONTRACT_ACCOUNT = os.getenv('CONTRACT_ACCOUNT', '')
CONTRACT_ACTION = os.getenv('CONTRACT_ACTION', '')
SUBMISSION_ACCOUNT = os.getenv('SUBMISSION_ACCOUNT', '')
SUBMISSION_PERMISSION = os.getenv('SUBMISSION_PERMISSION', '')
DATA_PERIOD_SECONDS = int(os.getenv('DATA_PERIOD_SECONDS', 24*3600))

# scheduling constants
SUBMISSION_INTERVAL_SECONDS = 10

# for gracefully handling docker signals
KEEP_RUNNING = True
def stop_container():
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

def seconds_to_time_string(epochsecs):
    return datetime.fromtimestamp(epochsecs).strftime('%Y-%m-%d %H:%M')

def get_current_data_submission_state():
    '''Returns the current period_start and state (whether waiting for totals or individual account usage)'''
    data = myencode({"scope": CONTRACT_ACCOUNT, "code": CONTRACT_ACCOUNT, "table": 'resourceconf', "json": True})
    data = json.dumps(data).encode("utf-8")
    try:
        table_info = requests.post(PUSH_API_NODE + '/v1/chain/get_table_rows', data=data, timeout=10).json()
        rows = table_info.get('rows', [])
        period_start = datetime.strptime(rows[0]['period_start'], '%Y-%m-%dT%H:%M:%S').timestamp()
        inflation_transferred = bool(rows[0]['inflation_transferred'])
        return period_start, 'INDIVIDUAL_USAGE' if inflation_transferred else 'TOTAL_USAGE'
    except:
        return None, ''

def get_expected_dataset_id():
    '''Returns the next dataset that is expected by the contract'''
    data = myencode({"scope": CONTRACT_ACCOUNT, "code": CONTRACT_ACCOUNT, "table": 'ressysusage', 
                        "lower_bound": f' {SUBMISSION_ACCOUNT}', "limit": 1, "json": True})
    data = json.dumps(data).encode("utf-8")
    try:
        table_info = requests.post(PUSH_API_NODE + '/v1/chain/get_table_rows', data=data, timeout=10).json()
        rows = table_info.get('rows', [])
        submission_hash_list = rows[0]['submission_hash_list']
        return len(submission_hash_list)
    except:
        return None

# wait for redis dump file to signify that DB is ready
while KEEP_RUNNING and not os.path.exists('/data/dump.rdb'):
    time.sleep(1)

# check for complete periods of data and submit at the appropriate time in contract lifecycle
while KEEP_RUNNING:
    # get current data submission period from contract
    current_time_seconds = datetime.now().timestamp()
    period_start_seconds, state = get_current_data_submission_state()

    data = r.get('SUBMISSION_DATA_' + str(period_start_seconds))
    if data:
        data = json.loads(data.decode('utf-8'))
        logger.info(data)
        dataset_id = get_expected_dataset_id()
        if dataset_id == 0: # send totals
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
                    "total_usage_hash": data['total_usage_hash'],
                    "all_data_hash": data['all_data_hash'],
                    "period_start": previous_date_start.strftime('%Y-%m-%dT%H:%M:%S')
                }
            }
            logger.info(f'Submitting resource usage totals for {seconds_to_time_string(period_start_seconds)}...')
            tx = {'actions': [action]}
            logger.info(tx)
            response = requests.post('http://eosjsserver:3000/push_transaction', json=tx, timeout=10).json()
            logger.info(f'Transaction {response["transaction_id"]} successfully submitted!')

        else: # send individual accounts dataset
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
                    "period_start": previous_date_start.strftime('%Y-%m-%dT%H:%M:%S')
                }
            }
            logger.info(f'Submitting accounts resource usage for {seconds_to_time_string(period_start_seconds)}...')
            tx = {'actions': [action]}
            logger.info(tx)
            response = requests.post('http://eosjsserver:3000/push_transaction', json=tx, timeout=10).json()
            logger.info(f'Transaction {response["transaction_id"]} successfully submitted!')


    time.sleep(SUBMISSION_INTERVAL_SECONDS)