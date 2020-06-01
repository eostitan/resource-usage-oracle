import csv
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
BLOCKS_API_NODE = os.getenv('EOSIO_BLOCKS_API_NODE', '')
CONTRACT_ACCOUNT = os.getenv('CONTRACT_ACCOUNT', '')
CONTRACT_ACTION = os.getenv('CONTRACT_ACTION', '')
SUBMISSION_ACCOUNT = os.getenv('SUBMISSION_ACCOUNT', '')
SUBMISSION_PERMISSION = os.getenv('SUBMISSION_PERMISSION', '')
EMPTY_DB_START_BLOCK = os.getenv('EMPTY_DB_START_BLOCK', '')
EXCLUDED_ACCOUNTS = os.getenv('EXCLUDED_ACCOUNTS','').split(',')

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
            start_block_num = start_block_num - blocks_since_midnight - (2 * 3600 * 24) - 120
        redis.set('last_block', start_block_num)
        logger.info('Database Initialised!')
    except:
        logger.error('Could not initialise database!')
        logger.error(traceback.format_exc())

# submit data to contract according to scheduling constants
def submit_resource_usage():
    try:
        response = {}
        t = datetime.utcnow()
        current_date_start = datetime(t.year, t.month, t.day, tzinfo=None)
        last_block_time = datetime.utcfromtimestamp(int(redis.get('last_block_time_seconds')))
        previous_date_start = current_date_start - timedelta(days=1)
        previous_date_string = previous_date_start.strftime("%Y-%m-%d")
        previous_date_accounts = [key[:-12] for key in redis.hkeys(previous_date_string) if key[-12:] == '-cpu-current']

        if last_block_time >= current_date_start:
            current_date_string = current_date_start.strftime("%Y-%m-%d")
            current_date_accounts = [key[:-12] for key in redis.hkeys(current_date_string) if key[-12:] == '-cpu-current']
            logger.info(f'Collating todays records... {len(current_date_accounts)} accounts so far.')

            if len(previous_date_accounts) > 0:

                # if totals for previous date haven't been sent, calculate and send them now
                if redis.get('last_usage_total_sent') != previous_date_string:
                    total_cpu_usage_us = 0
                    total_net_usage_words = 0
                    for account in previous_date_accounts:
                        total_cpu_usage_us += int(redis.hget(previous_date_string, f'{account}-cpu-current'))
                        total_net_usage_words += int(redis.hget(previous_date_string, f'{account}-net-current'))
                    action = {
                        "account": CONTRACT_ACCOUNT,
                        "name": "settotalusg",
                        "authorization": [{
                            "actor": SUBMISSION_ACCOUNT,
                            "permission": SUBMISSION_PERMISSION,
                        }],
                        "data": {"source": SUBMISSION_ACCOUNT,
                            "total_cpu_us": total_cpu_usage_us,
                            "total_net_words": total_net_usage_words,
                            "period_start": previous_date_start.strftime('%Y-%m-%dT%H:%M:%S')
                        }
                    }
                    logger.info(f'Submitting resource usage totals for {previous_date_string}...')
                    tx = {'actions': [action]}
                    logger.info(tx)
                    response = requests.post('http://eosjsserver:3000/push_transaction', json=tx, timeout=10).json()
                    logger.info(f'Transaction {response["transaction_id"]} successfully submitted!')
                    redis.set('last_usage_total_sent', previous_date_string)
                    time.sleep(5)

                # send ubsubmitted data
                actions = []
                for account in previous_date_accounts[:MAX_ACCOUNTS_PER_SUBMISSION]:
                    cpu_usage_us = redis.hget(previous_date_string, f'{account}-cpu-current')
                    action = {
                        "account": CONTRACT_ACCOUNT,
                        "name": "addactusg",
                        "authorization": [{
                            "actor": SUBMISSION_ACCOUNT,
                            "permission": SUBMISSION_PERMISSION,
                        }],
                        "data": {"source": SUBMISSION_ACCOUNT,
                            "dataset_id": 1,
                            "data": [
                                {"a": account, "u": cpu_usage_us}
                            ],
                            "period_start": previous_date_start.strftime('%Y-%m-%dT%H:%M:%S')
                        }
                    }
                    actions.append(action)

                logger.info(f'Submitting resource usage stats for {previous_date_string}...')
                tx = {'actions': actions}
                logger.info(tx)
                response = requests.post('http://eosjsserver:3000/push_transaction', json=tx, timeout=10).json()
                logger.info(f'Transaction {response["transaction_id"]} successfully submitted!')

                # remove data from -current once successfully sent
                for account in previous_date_accounts[:MAX_ACCOUNTS_PER_SUBMISSION]:
                    redis.hdel(previous_date_string, f'{account}-cpu-current')
                    redis.hdel(previous_date_string, f'{account}-net-current')

                # todo - handle if tx doesn't get included in immutable block?

        # if last block was yesterday, then aggregation is not finished, so don't submit
        if last_block_time < current_date_start:
            if len(previous_date_accounts) > 0:
                logger.info(f'Collating yesterdays records... {len(previous_date_accounts)} accounts so far.')


    except Exception as e:
        logger.error('Could not submit tx!')
        logger.error(response.get('error', traceback.format_exc()))

# prune redis database, keeping only the last 7 days worth
def prune_data():
    try:
        for key in redis.keys():
            if not key in ['last_block', 'last_block_time_seconds', 'last_usage_total_sent']:
                if datetime.strptime(key, '%Y-%m-%d') < datetime.utcnow() - timedelta(days=8):
                    redis.delete(key)
                    logger.info(f'Deleted old data from DB: {key}')
    except Exception as e:
        logger.info('Could not prune data!')
        logger.info(traceback.format_exc())

# exports all archived data to a single CSV file at redis/account-usage.csv
def export_data_to_csv():
    try:
        records = []
        for key in redis.keys():
            if not key in ['last_block', 'last_block_time_seconds', 'last_usage_total_sent']:
                accounts = list(set([key[:-12] for key in redis.hkeys(key)]))
                for account in accounts:
                    cpu_usage_us = redis.hget(key, f'{account}-cpu-archive')
                    net_usage_words = redis.hget(key, f'{account}-net-archive')
                    record = {'date': key, 'account': account, 'cpu_usage_us': cpu_usage_us, 'net_usage_words': net_usage_words}
                    records.append(record)
        with open('/data/accounts-usage.csv', 'w', encoding='utf8', newline='') as output_file:
            fc = csv.DictWriter(output_file, fieldnames=records[0].keys())
            fc.writeheader()
            fc.writerows(records)
        logger.info('Exported DB to CSV!')
    except Exception as e:
        logger.info('Could not export data!')
        logger.info(traceback.format_exc())

scheduler = BackgroundScheduler()
scheduler.add_job(submit_resource_usage, 'interval', seconds=SUBMISSION_INTERVAL_SECONDS, id='submit_resource_usage')
scheduler.add_job(prune_data, 'interval', minutes=60, id='prune_data')
scheduler.add_job(export_data_to_csv, 'interval', minutes=15, id='export_data_to_csv')
scheduler.start()


def fetch_block_json(b_num):
    block_info = requests.post(BLOCKS_API_NODE + '/v1/chain/get_block', json={'block_num_or_id': b_num}, timeout=10).json()
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

                            if actor not in EXCLUDED_ACCOUNTS:
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

            last_block, last_block_time, date_account_resource_deltas = fetch_block_range(block_range)
            if last_block:
                # add resource usage to redis in atomic transaction
                pipe = redis.pipeline()
                for key in date_account_resource_deltas:
                    pipe.hincrby(key[0], f'{key[1]}-{key[2]}-current', date_account_resource_deltas[key])
                    pipe.hincrby(key[0], f'{key[1]}-{key[2]}-archive', date_account_resource_deltas[key])
                pipe.set('last_block', last_block)
                pipe.set('last_block_time_seconds', int((last_block_time - datetime.utcfromtimestamp(0)).total_seconds()))
                pipe.execute()
                logger.info(f'Last collected block: {last_block}/{last_block_time.strftime("%Y-%m-%dT%H:%M:%S")}')
            time.sleep(0.5)

    except Exception as e:
        logger.error(f'Failed to collect block range: {e}')
        time.sleep(10)
