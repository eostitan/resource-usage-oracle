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
import requests
import redis

# get environment variables
BLOCKS_API_NODE = os.getenv('EOSIO_BLOCKS_API_NODE', '')
EMPTY_DB_START_BLOCK = os.getenv('EMPTY_DB_START_BLOCK', '')
EXCLUDED_ACCOUNTS = os.getenv('EXCLUDED_ACCOUNTS','').split(',')
DATA_PERIOD_SECONDS = int(os.getenv('DATA_PERIOD_SECONDS', 24*3600))
DATASET_BATCH_SIZE =  int(os.getenv('DATASET_BATCH_SIZE', 100))

# block collection and scheduling constants
BLOCK_ACQUISITION_THREADS = 20
MAX_ACCOUNTS_PER_SUBMISSION = 10
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

# if no redis dump file is present, determine starting block number and initialise db
if not os.path.exists('/data/dump.rdb'):
    try:
        if EMPTY_DB_START_BLOCK:
            start_block_num = EMPTY_DB_START_BLOCK
        else:
            # estimate block num a couple of minutes before the start of yesterday
            start_block_num = requests.get(f'{BLOCKS_API_NODE}/v1/chain/get_info', timeout=5).json()["last_irreversible_block_num"]
            logger.info(start_block_num)
            now = datetime.utcnow()
            blocks_since_midnight = int((now - now.replace(hour=0, minute=0, second=0, microsecond=0)).total_seconds() * 2)
#            start_block_num = start_block_num - blocks_since_midnight - (2 * 3600 * 24) - 120
            start_block_num = start_block_num - blocks_since_midnight - (2 * 3600 * 24) + 25000
        redis.set('last_block', start_block_num)
        logger.info('Database Initialised!')
    except:
        logger.error('Could not initialise database!')
        logger.error(traceback.format_exc())

def seconds_to_time_string(epochsecs):
    return datetime.fromtimestamp(epochsecs).strftime('%Y-%m-%d %H:%M')

def fetch_block_json(b_num):
    block_info = requests.post(BLOCKS_API_NODE + '/v1/chain/get_block', json={'block_num_or_id': b_num}, timeout=10).json()
    return b_num, block_info

def fetch_block_range(block_range):
    try:
        original_last_block = block_range.start - 1
        with ThreadPoolExecutor(max_workers=BLOCK_ACQUISITION_THREADS) as executor:
            results = executor.map(fetch_block_json,  block_range)
        results = sorted(results, key=lambda x: x[0]) # sort results by block number

        block_period_start = None
        date_account_resource_deltas = Counter()
        for result in results:
            block_number, block_info = result

            try:
                if not isinstance(block_number, int):
                    raise Exception('Returned block_number is not an Integer as expected.')

                new_block_time_seconds = datetime.strptime(block_info['timestamp']+'000', '%Y-%m-%dT%H:%M:%S.%f').timestamp()
                new_block_period_start = int((int(new_block_time_seconds) // DATA_PERIOD_SECONDS) * DATA_PERIOD_SECONDS)

                # ensure that block range only includes blocks from a single period
                if block_period_start:
                    if new_block_period_start != block_period_start:
                        break

                block_period_start = new_block_period_start

                for itx, tx in enumerate(block_info['transactions']):
                    if tx['status'] == 'executed':
                        if isinstance(tx['trx'], dict): # if not, is empty
                            actions = []
                            try:
                                actions = tx['trx']['transaction']['actions']
                            except Exception as e:
                                logger.info(traceback.format_exc())
                            actor = None
                            for action in actions:
                                try:
                                    actor = action['authorization'][0]['actor']
                                    break
                                except Exception as e:
                                    pass # Action has no auth actor, so will next available in tx

                            if actor not in EXCLUDED_ACCOUNTS:
                                date_account_resource_deltas[('AGGREGATION_DATA_' + str(block_period_start), actor, 'cpu')] += tx["cpu_usage_us"]
                                date_account_resource_deltas[('AGGREGATION_DATA_' + str(block_period_start), actor, 'net')] += tx["net_usage_words"]

            except Exception as e:
                logger.error(traceback.format_exc())

        return block_number, block_period_start, date_account_resource_deltas

    except Exception as e:
        logger.error(traceback.format_exc())
        return None, None

def aggregate_period_data(period_start):
    logger.info(f'Aggregating data for period {seconds_to_time_string(period_start)}')

    period_accounts = sorted([key[:-12] for key in redis.hkeys('AGGREGATION_DATA_' + str(period_start)) if key[-12:] == '-cpu-current'])

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
                    cpu_usage = int(redis.hget('AGGREGATION_DATA_' + str(period_start), f'{account}-cpu-current'))
                    net_usage = int(redis.hget('AGGREGATION_DATA_' + str(period_start), f'{account}-net-current'))
                    individual_usage_data.append({'a': account, 'u': cpu_usage})
                    individual_usage_hash_string += account + str(cpu_usage)
                    total_cpu_usage_us += cpu_usage
                    total_net_usage_words += net_usage
            else:
                pass # finished
            usage_datasets.append(individual_usage_data)
            usage_dataset_hashes.append(hashlib.sha256(individual_usage_hash_string.encode("utf8")).hexdigest())

    total_usage_hash = hashlib.sha256((str(total_cpu_usage_us) + str(total_net_usage_words)).encode("utf8")).hexdigest()
    all_data_hash = hashlib.sha256(''.join(usage_dataset_hashes).encode("utf8")).hexdigest()

    data = {
        'total_cpu_usage_us': total_cpu_usage_us,
        'total_net_usage_words': total_net_usage_words,
        'total_usage_hash': total_usage_hash,
        'all_data_hash': all_data_hash,
        'usage_datasets': usage_datasets
    }

    # temporary debugging
    logger.info('Usage Datasets')
    logger.info(usage_datasets)
#    logger.info(usage_dataset_hashes)
    logger.info(f'Total CPU: {total_cpu_usage_us}, Total NET: {total_net_usage_words}, Totals Hash: {total_usage_hash}, All Data hash: {all_data_hash}')

    # remove from AGGREGATION_DATA and add to SUBMISSION_DATA
    p = redis.pipeline()
    p.set('SUBMISSION_DATA_' + str(period_start), json.dumps(data))
    for account in period_accounts:
        p.delete('AGGREGATION_DATA_' + str(period_start))
    p.execute()


while KEEP_RUNNING:
    # get last irreversible block number from chain so we don't collect reversible blocks
    try:
        lib_number = requests.get(f'{BLOCKS_API_NODE}/v1/chain/get_info', timeout=5).json()["last_irreversible_block_num"]
    except Exception as e:
        logger.info(f'Failed to get last irreversible block - {e}')
        time.sleep(5)
        continue

    # get last block for which data has been collected
    try:
        last_block = int(redis.get('last_block'))
    except Exception as e:
        logger.error(f'Failed to get last block number in DB. It may not be Initialised.')
        logger.info('Will try again in 10 seconds...')
        time.sleep(10)
        continue

    # loop to collect a range of blocks resource usage data every cycle
    try:
        while KEEP_RUNNING:
            # get BLOCK_ACQUISITION_THREADS blocks of data
            block_range = range(last_block+1, min(last_block + BLOCK_ACQUISITION_THREADS + 1, lib_number))
            if len(block_range) < 1:
                logger.info('Reached last irreversible block - paused for 10 seconds...')
                time.sleep(10)
                logger.info('Restarting block collection...')
                break

            last_block, last_block_period_start, date_account_resource_deltas = fetch_block_range(block_range)
            if last_block:

                # if the data belongs to the next day and there is previous data, aggregate it for sending to contract
                redis_last_block_period_start = redis.get('last_block_period_start')
                if redis_last_block_period_start:
                    if last_block_period_start > int(redis_last_block_period_start):
                        aggregate_period_data(int(redis_last_block_period_start))

                # add resource usage to redis in atomic transaction
                pipe = redis.pipeline()
                for key in date_account_resource_deltas:
                    pipe.hincrby(key[0], f'{key[1]}-{key[2]}-current', date_account_resource_deltas[key])
                    pipe.hincrby(key[0], f'{key[1]}-{key[2]}-archive', date_account_resource_deltas[key])
                pipe.set('last_block', last_block)
                pipe.set('last_block_period_start', last_block_period_start)
                pipe.execute()
                logger.info(f'Collected Block: {last_block} / Aggregation Period: {seconds_to_time_string(last_block_period_start)}')
            time.sleep(0.5)

    except Exception as e:
        logger.error(f'Failed to collect block range: {e}')
        time.sleep(10)
