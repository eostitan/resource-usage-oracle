# core imports
import logging
import traceback
import json
import os
import time
import signal
import hashlib
from collections import Counter
from datetime import datetime, timedelta, date
from concurrent.futures import ThreadPoolExecutor

# external library imports
import requests
import redis

# app level imports
from utils import seconds_to_time_string, get_contract_configuration_state

# get environment variables
TEST_USAGE_DATA =  os.getenv('TEST_USAGE_DATA', 'False') == 'True'
TEST_USAGE_DATA_UTILITY_PERCENTAGE = int(os.getenv('TEST_USAGE_DATA_UTILITY_PERCENTAGE', 10))
TEST_USAGE_DATA_PERIODS = int(os.getenv('TEST_USAGE_DATA_PERIODS', 10))

if not TEST_USAGE_DATA:
    quit()

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

def aggregate_period_test_data(period_start):
    logger.info(f'Creating test data for period {seconds_to_time_string(period_start)} at {TEST_USAGE_DATA_UTILITY_PERCENTAGE}% utility')

    period_accounts = ['bp1', 'bp2', 'bp3', 'bp4', 'bp5', 'bpa', 'bpb', 'bpc', 'bpd', 'bpe']

    total_cpu_usage_us = 0
    total_net_usage_words = 0
    usage_datasets = [[]]
    usage_dataset_hashes = []
    if len(period_accounts) > 0:
        for i in range(0, len(period_accounts), DATASET_BATCH_SIZE):
            individual_usage_data = []
            individual_usage_hash_string = ''
            accounts = period_accounts[i:i+DATASET_BATCH_SIZE]
            if len(accounts) > 0:
                for account in accounts:
                    cpu_usage = int((345600000 * TEST_USAGE_DATA_UTILITY_PERCENTAGE) / 10)
                    net_usage = int((226492416 * TEST_USAGE_DATA_UTILITY_PERCENTAGE) / 10)
                    individual_usage_data.append({'a': account, 'u': cpu_usage})
                    individual_usage_hash_string += account + str(cpu_usage)
                    total_cpu_usage_us += cpu_usage
                    total_net_usage_words += net_usage
                usage_datasets.append(individual_usage_data)
                usage_dataset_hashes.append(hashlib.sha256(individual_usage_hash_string.encode("utf8")).hexdigest())

    total_usage_hash = hashlib.sha256((str(total_cpu_usage_us) + '-' + str(total_net_usage_words)).encode("utf8")).hexdigest()
    usage_dataset_hashes = [total_usage_hash] + usage_dataset_hashes
    all_data_hash = hashlib.sha256(('-'.join(usage_dataset_hashes)).encode("utf8")).hexdigest()

    data = {
        'total_cpu_usage_us': total_cpu_usage_us,
        'total_net_usage_words': total_net_usage_words,
        'total_usage_hash': total_usage_hash,
        'all_data_hash': all_data_hash,
        'usage_datasets': usage_datasets
    }

    logger.info(f'Total CPU: {total_cpu_usage_us}, Total NET: {total_net_usage_words}, Totals Hash: {total_usage_hash}, All Data hash: {all_data_hash}')

    # add to SUBMISSION_DATA
    p = redis.pipeline()
    p.set('SUBMISSION_DATA_' + str(period_start), json.dumps(data))
    p.execute()


logger.info('RUNNING USING TEST USAGE DATA!')

# check no existing redis db and initialise
if os.path.exists('/data/dump.rdb'):
    logger.error('Cannot continue with testing. Please clear redis data and restart.')
    KEEP_RUNNING = False

period_start_seconds, DATA_PERIOD_SECONDS, DATASET_BATCH_SIZE, _ = get_contract_configuration_state()
redis.set('DATA_PERIOD_SECONDS', DATA_PERIOD_SECONDS)
redis.set('DATASET_BATCH_SIZE', DATASET_BATCH_SIZE)
redis.save()

for period in range(TEST_USAGE_DATA_PERIODS):
    aggregate_period_test_data(int(period_start_seconds) + (period * DATA_PERIOD_SECONDS))