import logging
import traceback
import json
import os
import time
import signal
from datetime import datetime, timedelta, date
from threading import Thread
from multiprocessing.dummy import Pool as ThreadPool
import requests
import redis
import pandas as pd
from apscheduler.schedulers.background import BackgroundScheduler


API_NODES = (os.getenv('EOS_API_NODE_1', ''), os.getenv('EOS_API_NODE_2', ''))
SIMULTANEOUS_BLOCKS = 10
EMPTY_TABLE_START_BLOCK = 72655480

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
handler = logging.FileHandler('debug.log')
handler.setLevel(logging.INFO)
formatter = logging.Formatter(f'%(asctime)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)

redis = redis.StrictRedis(host='redis', port=6379, db=0, decode_responses=False)

def myencode(data):
    return json.dumps(data).encode("utf-8")

def submit_usage():
    logger.info('Submitting Usage!')

scheduler = BackgroundScheduler()
scheduler.add_job(submit_usage, 'interval', minutes=1, id='submit_usage')
scheduler.start()


KEEP_RUNNING = True
def stop_container():
    global KEEP_RUNNING
    KEEP_RUNNING = False
signal.signal(signal.SIGTERM, stop_container)


def fetchBlockJSON(b_num):
    data = myencode({'block_num_or_id': b_num})
    block_info = requests.post(API_NODES[0] + '/v1/chain/get_block', data=data, timeout=10).json()
    return b_num, block_info

def fetchBlockRange(block_range):
    try:
        original_last_block = block_range.start - 1
        pool = ThreadPool(SIMULTANEOUS_BLOCKS)
        results = pool.map(fetchBlockJSON,  block_range)
        pool.close()
        pool.join()
        results = sorted(results, key=lambda x: x[0])

        for result in results:
            block_number, block_info = result

            try:
                if not isinstance(block_number, int):
                    raise Exception('Returned block_number is not an Integer as expected.')

                block_time = datetime.strptime(block_info['timestamp']+'000', '%Y-%m-%dT%H:%M:%S.%f')

                for itx, tx in enumerate(block_info['transactions']):
                    if tx['status'] == 'executed':
                        if isinstance(tx['trx'], dict): # if not, is empty
                            actions = []
                            try:
                                actions = tx['trx']['transaction']['actions']
                            except Exception as e:
                                logger.error(e)
                            actor = None
                            for action in actions:
                                try:
                                    actor = action['authorization'][0]['actor']
                                    break
                                except Exception as e:
                                    logger.error(e)

                            # add resource usage to redis
                            logger.info(f'{actor} - {tx["cpu_usage_us"]} - {tx["net_usage_words"]}')

            except Exception as e:
                logger.error(e)

    except Exception as e:
        logger.error(e)



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
#        logger.info(traceback.format_exc())
        time.sleep(1)

    try:
        while KEEP_RUNNING:
            # get n blocks of data
            target_end_block = chain_info['last_irreversible_block_num']
#            if self.pause_at_block:
#                target_end_block = min(target_end_block, self.pause_at_block)
            block_range = range(last_block+1, min(last_block + SIMULTANEOUS_BLOCKS + 1, target_end_block))
            if len(block_range) < 1:
                logger.info('Pausing Block Collection Thread')
                time.sleep(5)
                logger.info('Restarting Block Collection Thread')
                break

            last_block = fetchBlockRange(block_range)
            if last_block:
                redis.set('last_block', last_block)
            time.sleep(0.5)

    except Exception as e:
        logger.error(e)

