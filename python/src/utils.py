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

def get_current_data_submission_state():
    '''Returns the current period_start and state (whether waiting for totals or individual account usage)'''
    data = {"scope": CONTRACT_ACCOUNT, "code": CONTRACT_ACCOUNT, "table": 'resourceconf', "json": True}
    data = json.dumps(data).encode("utf-8")
    try:
        table_info = requests.post(PUSH_API_NODE + '/v1/chain/get_table_rows', data=data, timeout=10).json()
        logger.info(table_info)
        rows = table_info.get('rows', [])
        period_start = datetime.strptime(rows[0]['period_start'], '%Y-%m-%dT%H:%M:%S').timestamp()
        inflation_transferred = bool(rows[0]['inflation_transferred'])
        return period_start, 'INDIVIDUAL_USAGE' if inflation_transferred else 'TOTAL_USAGE'
    except:
        logger.info(f'get_current_data_submission_state failed')
        logger.info(traceback.format_exc())
        return None, ''

def get_expected_dataset_id():
    '''Returns the next dataset that is expected by the contract'''
    data = {"scope": CONTRACT_ACCOUNT, "code": CONTRACT_ACCOUNT, "table": 'ressysusage',
                        "lower_bound": f' {SUBMISSION_ACCOUNT}', "limit": 1, "json": True}
    data = json.dumps(data).encode("utf-8")
    try:
        table_info = requests.post(PUSH_API_NODE + '/v1/chain/get_table_rows', data=data, timeout=10).json()
        logger.info(table_info)
        rows = table_info.get('rows', [])
        submission_hash_list = rows[0]['submission_hash_list']
        return len(submission_hash_list)
    except:
        logger.info(f'get_expected_dataset_id failed')
        logger.info(traceback.format_exc())
        return None
