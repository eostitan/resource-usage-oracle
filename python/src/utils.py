# core imports
import logging
import traceback
import json
import os
from datetime import datetime, timedelta, date

# external library imports
import requests
import redis

# get environment variables
PUSH_API_NODE = os.getenv('EOSIO_PUSH_API_NODE', '')
CONTRACT_ACCOUNT = os.getenv('CONTRACT_ACCOUNT', '')
SUBMISSION_ACCOUNT = os.getenv('SUBMISSION_ACCOUNT', '')
BLOCKS_API_NODE = os.getenv('EOSIO_BLOCKS_API_NODE', '')

# logging configuration
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
handler = logging.FileHandler('debug.log')
handler.setLevel(logging.INFO)
formatter = logging.Formatter(f'%(asctime)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)

# helper functions
def seconds_to_time_string(epochsecs):
    '''Returns time as formatted string'''
    return datetime.fromtimestamp(epochsecs).strftime('%Y-%m-%d %H:%M')

def get_contract_configuration_state():
    '''
        Returns period_start, period_seconds, dataset_batch_size and state from contract
        state = whether waiting for totals or individual account usage)
    '''
    data = {"scope": CONTRACT_ACCOUNT, "code": CONTRACT_ACCOUNT, "table": 'resourceconf', "json": True}
    data = json.dumps(data).encode("utf-8")
    try:
        table_info = requests.post(PUSH_API_NODE + '/v1/chain/get_table_rows', data=data, timeout=10).json()
        rows = table_info.get('rows', [])
        period_start = datetime.strptime(rows[0]['period_start'], '%Y-%m-%dT%H:%M:%S').timestamp()
        period_seconds = int(rows[0]['period_seconds'])
        dataset_batch_size = int(rows[0]['dataset_batch_size'])
        inflation_transferred = bool(rows[0]['inflation_transferred'])
        return period_start, period_seconds, dataset_batch_size, 'INDIVIDUAL_USAGE' if inflation_transferred else 'TOTAL_USAGE'
    except:
        logger.info(f'get_contract_configuration failed')
        logger.info(traceback.format_exc())
        return None, ''

def get_contract_expected_dataset_id():
    '''Returns the next dataset that is expected by the contract'''
    data = {"scope": CONTRACT_ACCOUNT, "code": CONTRACT_ACCOUNT, "table": 'ressysusage',
                        "lower_bound": f' {SUBMISSION_ACCOUNT}', "limit": 1, "json": True}
    data = json.dumps(data).encode("utf-8")
    try:
        table_info = requests.post(PUSH_API_NODE + '/v1/chain/get_table_rows', data=data, timeout=10).json()
        rows = table_info.get('rows', [])
        source = rows[0]['source']
        submission_hash_list = rows[0]['submission_hash_list']
        if source == SUBMISSION_ACCOUNT: # found oracle row
            return len(submission_hash_list)
        else: # no datasets submitted for this account
            return 0
    except IndexError as e: # no rows
        return 0
    except:
        logger.info(f'get_expected_dataset_id failed')
        logger.info(traceback.format_exc())
        return -1

def seconds_to_block_number(epochsecs):
    '''Given time in epoch seconds, returns approximate block'''
    current_time_seconds = datetime.now().timestamp()
    # estimate block num a couple of minutes before epochsecs
    lib_block_num = requests.get(f'{BLOCKS_API_NODE}/v1/chain/get_info', timeout=5).json()["last_irreversible_block_num"]
    target_block_num = lib_block_num - ((current_time_seconds - epochsecs) * 2)
    # TODO - check block is before target time, to ensure complete data

    return int(target_block_num)