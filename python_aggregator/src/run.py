import logging
import traceback
import json
import os
import time
import signal
from collections import Counter
from datetime import datetime, timedelta, date
import pytz
from threading import Thread
from multiprocessing.dummy import Pool as ThreadPool
import requests
import redis
import pandas as pd
from apscheduler.schedulers.background import BackgroundScheduler
import eospy.cleos
from eospy.cleos import EOSKey


API_NODES = (os.getenv('EOSIO_API_NODE_1', ''), os.getenv('EOSIO_API_NODE_2', ''))
CONTRACT_ACCOUNT = os.getenv('CONTRACT_ACCOUNT', '')
CONTRACT_ACTION = os.getenv('CONTRACT_ACTION', '')
SUBMISSION_ACCOUNT = os.getenv('SUBMISSION_ACCOUNT', '')
SUBMISSION_PERMISSION = os.getenv('SUBMISSION_PERMISSION', '')
SUBMISSION_PUBLIC_KEY = os.getenv('SUBMISSION_PUBLIC_KEY', '')
SUBMISSION_PRIVATE_KEY = os.getenv('SUBMISSION_PRIVATE_KEY', '')

SIMULTANEOUS_BLOCKS = 10
EMPTY_TABLE_START_BLOCK = 72655480
MAX_ACCOUNTS_PER_SUBMISSION = 10

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
handler = logging.FileHandler('debug.log')
handler.setLevel(logging.INFO)
formatter = logging.Formatter(f'%(asctime)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)

redis = redis.StrictRedis(host='redis', port=6379, db=0, decode_responses=True)
ce = eospy.cleos.Cleos(url=API_NODES[0])


def myencode(data):
    return json.dumps(data).encode("utf-8")

def send_transaction(actions):
    tx = {'actions': actions}
    response = requests.post('http://eosjsserver:3000/push_transaction', json=tx, timeout=20).json()
    logger.info(response)
    return None


def submit_resource_usage():
    try:
        previous_date_string = (datetime.utcnow() - timedelta(days=2)).strftime("%Y-%m-%d")
        records = []
        active_accounts = list(set(redis.hkeys(previous_date_string)))
        actions = []
        for key in active_accounts[:MAX_ACCOUNTS_PER_SUBMISSION]:
            actor = key[:-4]
            cpu_usage_us = redis.hget(previous_date_string, f'{actor}-cpu')
            net_usage_words = redis.hget(previous_date_string, f'{actor}-net')
            record = {'account': actor, 'cpu_usage_us': cpu_usage_us, 'net_usage_words': net_usage_words}
            records.append(record)

            action = {
                "account": CONTRACT_ACCOUNT,
                "name": CONTRACT_ACTION,
                "authorization": [{
                    "actor": SUBMISSION_ACCOUNT,
                    "permission": SUBMISSION_PERMISSION,
                }],
                "data": {"source": SUBMISSION_ACCOUNT,
                    "account": actor, 
                    "cpu_quantity": cpu_usage_us,
                    "net_quantity": net_usage_words}
            }
            actions.append(action)

        logger.info(f'Submitting resource usage stats for {previous_date_string}...')
#        send_transaction(actions, SUBMISSION_PRIVATE_KEY)
        send_transaction(actions)
        logger.info('Submitted resource usage stats!')

        # remove data once successfully sent
        # todo - handle if tx doesn't get included in immutable block
        for key in active_accounts[:MAX_ACCOUNTS_PER_SUBMISSION]:
            actor = key[:-4]
            redis.hdel(previous_date_string, f'{actor}-cpu')
            redis.hdel(previous_date_string, f'{actor}-net')

    except Exception as e:
        logger.info('Could not submit tx!')
        logger.info(traceback.format_exc())


scheduler = BackgroundScheduler()
scheduler.add_job(submit_resource_usage, 'interval', seconds=10, id='submit_resource_usage')
scheduler.start()


KEEP_RUNNING = True
def stop_container():
    global KEEP_RUNNING
    KEEP_RUNNING = False
signal.signal(signal.SIGTERM, stop_container)


def fetch_block_json(b_num):
    data = myencode({'block_num_or_id': b_num})
    block_info = requests.post(API_NODES[0] + '/v1/chain/get_block', data=data, timeout=10).json()
    return b_num, block_info

def fetch_block_range(block_range):
    try:
        original_last_block = block_range.start - 1
        pool = ThreadPool(SIMULTANEOUS_BLOCKS)
        results = pool.map(fetch_block_json,  block_range)
        pool.close()
        pool.join()
        results = sorted(results, key=lambda x: x[0])

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
                                    logger.info('Action has no auth actor, using next available...')

                            date_account_resource_deltas[(block_date_string, actor, 'cpu')] += tx["cpu_usage_us"]
                            date_account_resource_deltas[(block_date_string, actor, 'net')] += tx["net_usage_words"]

            except Exception as e:
                logger.error(traceback.format_exc())

        return block_number, date_account_resource_deltas

    except Exception as e:
        logger.error(traceback.format_exc())
        return None, None


time.sleep(3)
while KEEP_RUNNING:
    try:
        chain_info = requests.get(f'{API_NODES[0]}/v1/chain/get_info', timeout=5).json()
        logger.info('Last Irreversible Block:' + str(chain_info['last_irreversible_block_num']))
    except Exception as e:
        logger.info('Failed to get last irr block in chain: ' + str(e))
        time.sleep(1)
        continue

    if EMPTY_TABLE_START_BLOCK > 0:
        last_block = EMPTY_TABLE_START_BLOCK - 1
    elif EMPTY_TABLE_START_BLOCK < 0:
        last_block = chain_info['last_irreversible_block_num'] + EMPTY_TABLE_START_BLOCK - 1

    try:
        last_block = int(redis.get('last_block'))
        logger.info('Last Block Number in Redis: ' + str(last_block))
    except Exception as e:
        logger.info('Failed to get last block in Redis: ' + str(e))
        logger.info('Using: ' + str(last_block))
        time.sleep(1)

    try:
        while KEEP_RUNNING:
            # get n blocks of data
            target_end_block = chain_info['last_irreversible_block_num']
            block_range = range(last_block+1, min(last_block + SIMULTANEOUS_BLOCKS + 1, target_end_block))
            if len(block_range) < 1:
                logger.info('Pausing Block Collection Thread')
                time.sleep(5)
                logger.info('Restarting Block Collection Thread')
                break

            last_block, date_account_resource_deltas = fetch_block_range(block_range)
            if last_block:
                # add resource usage to redis
                pipe = redis.pipeline()
                for key in date_account_resource_deltas:
                    pipe.hincrby(key[0], f'{key[1]}-{key[2]}', date_account_resource_deltas[key])
                pipe.set('last_block', last_block)
                pipe.execute()
                logger.info(last_block)
            time.sleep(0.5)

    except Exception as e:
        logger.error(e)

