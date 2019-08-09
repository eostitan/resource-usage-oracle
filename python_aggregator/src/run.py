import logging
import traceback
import json
import os
import time
import signal
from collections import Counter
from datetime import datetime, timedelta, date
import pytz
from concurrent.futures import ThreadPoolExecutor
import requests
import redis
from apscheduler.schedulers.background import BackgroundScheduler

# get environment variables
API_NODE = os.getenv('EOSIO_API_NODE', '')
CONTRACT_ACCOUNT = os.getenv('CONTRACT_ACCOUNT', '')
CONTRACT_ACTION = os.getenv('CONTRACT_ACTION', '')
SUBMISSION_ACCOUNT = os.getenv('SUBMISSION_ACCOUNT', '')
SUBMISSION_PERMISSION = os.getenv('SUBMISSION_PERMISSION', '')

# block collection and scheduling constants
EMPTY_TABLE_START_BLOCK = 72854758 # start of 8th Aug
BLOCK_ACQUISITION_THREADS = 10
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


# submit data to contract according to scheduling constants
def submit_resource_usage():
    try:
        t = datetime.utcnow()
        current_date_start = datetime(t.year, t.month, t.day, tzinfo=None)
        last_block_time = datetime.utcfromtimestamp(int(redis.get('last_block_time_seconds')))

        previous_date_string = (current_date_start - timedelta(days=1)).strftime("%Y-%m-%d")

        active_accounts = list(set([key[:-4] for key in redis.hkeys(previous_date_string)]))
        logger.info(f'Active accounts: {len(active_accounts)}')

        # if last block was yesterday, then aggregation is not finished, so don't submit
        if last_block_time < current_date_start:
            logger.info(f'No data ready to submit for {previous_date_string}')
            return

        records = []
        actions = []
        for account in active_accounts[:MAX_ACCOUNTS_PER_SUBMISSION]:
            cpu_usage_us = redis.hget(previous_date_string, f'{account}-cpu')
            net_usage_words = redis.hget(previous_date_string, f'{account}-net')
            record = {'account': account, 'cpu_usage_us': cpu_usage_us, 'net_usage_words': net_usage_words}
            records.append(record)

            action = {
                "account": CONTRACT_ACCOUNT,
                "name": CONTRACT_ACTION,
                "authorization": [{
                    "actor": SUBMISSION_ACCOUNT,
                    "permission": SUBMISSION_PERMISSION,
                }],
                "data": {"source": SUBMISSION_ACCOUNT,
                    "account": account, 
                    "cpu_quantity": cpu_usage_us,
                    "net_quantity": net_usage_words}
            }
            actions.append(action)

        logger.info(f'Submitting resource usage stats for {previous_date_string}...')
        tx = {'actions': actions}
        logger.info(tx)
    #    response = requests.post('http://eosjsserver:3000/push_transaction', json=tx, timeout=20).json()
    #    logger.info(response)
        logger.info('Submitted resource usage stats!')

        # remove data once successfully sent
        # todo - handle if tx doesn't get included in immutable block
        for account in active_accounts[:MAX_ACCOUNTS_PER_SUBMISSION]:
            redis.hdel(previous_date_string, f'{account}-cpu')
            redis.hdel(previous_date_string, f'{account}-net')

    except Exception as e:
        logger.info('Could not submit tx!')
        logger.info(traceback.format_exc())

scheduler = BackgroundScheduler()
scheduler.add_job(submit_resource_usage, 'interval', seconds=SUBMISSION_INTERVAL_SECONDS, id='submit_resource_usage')
scheduler.start()


def fetch_block_json(b_num):
    block_info = requests.post(API_NODE + '/v1/chain/get_block', json={'block_num_or_id': b_num}, timeout=10).json()
    return b_num, block_info

def fetch_block_range(block_range):
    try:
        original_last_block = block_range.start - 1
        with ThreadPoolExecutor(max_workers=BLOCK_ACQUISITION_THREADS) as executor:
            results = executor.map(fetch_block_json,  block_range)
        results = sorted(results, key=lambda x: x[0]) # sort results by block number

        date_account_resource_deltas = Counter()
        for result in results:
            block_number, block_info = result

            try:
                if not isinstance(block_number, int):
                    raise Exception('Returned block_number is not an Integer as expected.')

                block_time = datetime.strptime(block_info['timestamp']+'000', '%Y-%m-%dT%H:%M:%S.%f')
                block_date_string = block_time.strftime("%Y-%m-%d")

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

                            date_account_resource_deltas[(block_date_string, actor, 'cpu')] += tx["cpu_usage_us"]
                            date_account_resource_deltas[(block_date_string, actor, 'net')] += tx["net_usage_words"]

            except Exception as e:
                logger.error(traceback.format_exc())

        return block_number, block_time, date_account_resource_deltas

    except Exception as e:
        logger.error(traceback.format_exc())
        return None, None

while KEEP_RUNNING:
    # get last irreversible block number from chain so we don't collect reversible blocks
    try:
        lib_number = requests.get(f'{API_NODE}/v1/chain/get_info', timeout=5).json()["last_irreversible_block_num"]
    except Exception as e:
        logger.info(f'Failed to get last irreversible block - {e}')
        time.sleep(5)
        continue

    # handle absolute and relative block offsets for empty tables
    if EMPTY_TABLE_START_BLOCK > 0:
        last_block = EMPTY_TABLE_START_BLOCK - 1
    elif EMPTY_TABLE_START_BLOCK < 0:
        last_block = lib_number + EMPTY_TABLE_START_BLOCK - 1

    # get last block for which data has been collected
    try:
        last_block = int(redis.get('last_block'))
    except Exception as e:
        logger.info(f'Failed to get last block in Redis: {e}')
        logger.info(f'Using: {last_block}')
        time.sleep(5)

    # loop to collect a range of blocks resource usage data every cycle
    try:
        while KEEP_RUNNING:
            # get BLOCK_ACQUISITION_THREADS blocks of data
            block_range = range(last_block+1, min(last_block + BLOCK_ACQUISITION_THREADS + 1, lib_number))
            if len(block_range) < 1:
                logger.info('Reached last irreversible block - block collection paused')
                time.sleep(5)
                logger.info('Restarting block collection')
                break

            last_block, last_block_time, date_account_resource_deltas = fetch_block_range(block_range)
            if last_block:
                # add resource usage to redis in atomic transaction
                pipe = redis.pipeline()
                for key in date_account_resource_deltas:
                    pipe.hincrby(key[0], f'{key[1]}-{key[2]}', date_account_resource_deltas[key])
                pipe.set('last_block', last_block)
                pipe.set('last_block_time_seconds', int((last_block_time - datetime.utcfromtimestamp(0)).total_seconds()))
                pipe.execute()
                logger.info(f'Last collected block: {last_block}/{last_block_time.strftime("%Y-%m-%dT%H:%M:%S")}')
            time.sleep(0.5)

    except Exception as e:
        logger.error(f'Failed to collect block range: {e}')
