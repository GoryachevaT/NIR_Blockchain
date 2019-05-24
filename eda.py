import time
import asyncio
import aiofiles
import os
import ujson
import requests
import pandas as pd
from statistics import mean 
from aioblockexplorer import chunkify, fetch
from proxy_list import tr_list, addr_list
import sys
import math


CACHE_DIR_TX = '/tmp/btc_txids'
CACHE_DIR_ADDS = '/tmp/btc_adds'
os.makedirs(CACHE_DIR_TX, exist_ok=True)
os.makedirs(CACHE_DIR_ADDS, exist_ok=True)

curr_tr_list = tr_list
curr_add_list = addr_list
path_tx_fin = './tx_json.json'
path_add_fin = './add_json.json'


#характеристики блока и самой транзакции
def tr_features(row, tx):
    row['txid']    = tx['hash']
    row['confirmations'] = tx['confirmations']
    row['time'] = tx['block_time']
    row['is_coinbase'] = tx['is_coinbase']
    row['is_double_spend'] = tx['is_double_spend']
    row['is_sw_tx'] = tx['is_sw_tx']
    row['weight'] = tx['weight']
    row['vsize'] = tx['vsize'] 
    return row

#сумма транзакции + кол-во входных и выходных адресов
def inp_outp_features(row, tx):   
    row['inputs_count'] = tx['inputs_count']
    row['outputs_count'] = tx['outputs_count']
    row['inputs_value'] = tx['inputs_value']
    row['outputs_value'] = tx['outputs_value'] 
    inp_adds = []
    for inp in tx['inputs']:
        inp_adds.extend(inp['prev_addresses'])
    outp_adds = []
    for outp in tx['outputs']:
        outp_adds.extend(outp['addresses'])
    return row


async def get_tx(txid):
    # print('get_tx:', txid)
    path = f'{CACHE_DIR_TX}/{txid}.json'
    if os.path.exists(path):
        async with aiofiles.open(path, 'r') as f:
            data = ujson.loads(await f.read())
    else:
        data = await fetch(f'https://chain.api.btc.com/v3/tx/{txid}')
        async with aiofiles.open(path, 'w') as f:
            await f.write(ujson.dumps(data))
    if data is None or not data:
        return {"data": {"hash": txid}, "err_no": 666}
    return data


async def get_page(add, page):
    path = f'{CACHE_DIR_ADDS}/{add}'
    os.makedirs(path, exist_ok=True)
    data = await fetch(f'https://chain.api.btc.com/v3/address/{add}/tx?page={page}')
    async with aiofiles.open(path+f'/page_{page}.json', 'w') as f:
            await f.write(ujson.dumps(data))
    return data['data']['list']


async def get_tx_by_add(add):
#def get_tx_by_add(add):
    path = f'{CACHE_DIR_ADDS}/{add}'
    data = []

    if os.path.exists(path):
        files = os.listdir(path)
        for file in files:
            with open(path+f'/{file}', 'r') as f:
                tmp = ujson.loads(f.read())
            data.extend(tmp['data']['list'])
    else:
        os.makedirs(path, exist_ok=True)
        tmp = await fetch(f'https://chain.api.btc.com/v3/address/{add}/tx?page=1')
        async with aiofiles.open(path+'/page_1.json', 'w') as f:
            await f.write(ujson.dumps(tmp))

        
        total_count = tmp['data']['total_count']
        pagesize = 50
        pages = math.ceil(total_count/pagesize)
        
        print(f'total_count: {total_count}, pages: {pages}')
        data.extend(tmp['data']['list'])

        for page in range(2, pages+1):
            print(f'page {page} of {pages}')
            tmp = await get_page(add, page)
            data.extend(tmp)
            print('done!')

    return data



async def time_features(row, tx):
    new_txs = []
    err = []

    #previous transactions
    prev_times = []
    prev_vals = []
    inp = []
    #tasks = [get_tx(itx['prev_tx_hash']) for itx in tx['data']['inputs']]
    tasks = [get_tx(itx['prev_tx_hash']) for itx in tx['inputs']]
    prev_addresses = [{'inp_address': itx['prev_addresses'][0], 'curr_value': itx['prev_value']} for itx in tx['inputs']]
    chunks_size = 16
    chunks = list(chunkify(tasks, chunks_size))
    print('inputs:', len(tx['inputs']), 'chunks:', len(chunks))
    for chunk_i, chunk in enumerate(chunks, start=1):
        print(f'[chunk #{chunk_i}/{len(chunks)}]: fetching...')
        tmp_txs = await asyncio.gather(*chunk)
        for j, tmp_tx in enumerate(tmp_txs):
            txid = tmp_tx['data']['hash']
            new_txs.append(txid)
            if tmp_tx.get('err_no', 0):
                err.append(txid)
                prev_time = None
                prev_val = None
            else:
                prev_time = tmp_tx['data']['block_time']
                prev_val = tmp_tx['data']['inputs_value']
            prev_times.append(prev_time)
            prev_vals.append(prev_val)
            inp.append({'prev_address': prev_addresses[j+chunks_size*(chunk_i-1)]['inp_address'],
                        'curr_value': prev_addresses[j+chunks_size*(chunk_i-1)]['curr_value'],
                        'prev_time': prev_time,
                        'total_prev_val': prev_val
                        })
        print(f'[chunk #{chunk_i}/{len(chunks)}]: done!')

    prev_times = [x for x in prev_times if x]
    if prev_times:
        row['max_prev_time'] = max(prev_times)
        row['min_prev_time'] = min(prev_times)
        row['avg_prev_time'] = round(mean(prev_times))
    else: 
        row['max_prev_time'] = None
        row['min_prev_time'] = None
        row['avg_prev_time'] = None
    print(f'prev_time fin')
    prev_vals = [x for x in prev_vals if x]
    if prev_vals:
        row['max_prev_val'] = max(prev_vals)
        row['min_prev_val'] = min(prev_vals)
        row['avg_prev_val'] = round(mean(prev_vals))
    else: 
        row['max_prev_val'] = None
        row['min_prev_val'] = None
        row['avg_prev_val'] = None
    row['list_of_inp_adds'] = inp


    #next transactions
    next_times = []
    next_vals = []
    outp = []
    tasks = [get_tx(itx['spent_by_tx']) for itx in tx['outputs']]
    next_addresses = [{'outp_address': itx['addresses'][0], 'curr_value': itx['value']} for itx in tx['outputs']]
    chunks_size = 16
    chunks = list(chunkify(tasks, chunks_size))
    print('outputs:', len(tx['outputs']), 'chunks:', len(chunks))
    for chunk_i, chunk in enumerate(chunks, start=1):
        print(f'[chunk #{chunk_i}/{len(chunks)}]: fetching...')
        tmp_txs = await asyncio.gather(*chunk)
        for j, tmp_tx in enumerate(tmp_txs):
            try:
                txid = tmp_tx['data']['hash']
                new_txs.append(txid)
            except: pass
            if tmp_tx.get('err_no', 0):
                err.append(txid)
                next_time = None
                next_val = None
            else:
                next_time = tmp_tx['data']['block_time']
                next_val = tmp_tx['data']['inputs_value']
            next_times.append(next_time)
            next_vals.append(next_val)
            outp.append({'prev_address': next_addresses[j+chunks_size*(chunk_i-1)]['outp_address'], #!!!!!изменить на next_address
                        'curr_value': next_addresses[j+chunks_size*(chunk_i-1)]['curr_value'],
                        'next_time': next_time,
                        'total_next_val': next_val
                        })
        print(f'[chunk #{chunk_i}/{len(chunks)}]: done!')

    next_times = [x for x in next_times if x]
    if next_times:
        row['max_next_time'] = max(next_times)
        row['min_next_time'] = min(next_times)
        row['avg_next_time'] = round(mean(next_times))
    else: 
        row['max_next_time'] = None
        row['min_next_time'] = None
        row['avg_next_time'] = None
    next_vals = [x for x in next_vals if x]
    if next_vals:
        row['max_next_val'] = max(next_vals)
        row['min_next_val'] = min(next_vals)
        row['avg_next_val'] = round(mean(next_vals))
    else: 
        row['max_next_val'] = None
        row['min_next_val'] = None
        row['avg_next_val'] = None

    row['list_of_outp_adds'] = outp
    return err, row, new_txs


def diff_time_features(row):
    if row['max_prev_time']: row['diff_max_prev_time'] = row['time'] - row['max_prev_time']
    else: row['diff_max_prev_time'] = None
    if row['min_prev_time']: row['diff_min_prev_time'] = row['time'] - row['min_prev_time']
    else: row['diff_min_prev_time'] = None
    if row['avg_prev_time']: row['diff_avg_prev_time'] = row['time'] - row['avg_prev_time']
    else: row['diff_avg_prev_time'] = None
    if row['max_next_time']: row['diff_max_next_time'] = row['max_next_time'] - row['time'] 
    else: row['diff_max_next_time'] = None
    if row['min_next_time']: row['diff_min_next_time'] = row['min_next_time'] - row['time']  
    else: row['diff_min_next_time'] = None
    if row['avg_next_time']: row['diff_avg_next_time'] = row['avg_next_time'] - row['time'] 
    else: row['diff_avg_next_time'] = None
    return row


async def get_by_transaction_list(n=4, curr_tr_list=curr_tr_list, path_fin=path_tx_fin):

    df = {}
    finished_txs = []

    for i in range(n): 
        next_level_txs = []
        print(f'level {i+1} of {n}')


        for j, txid in enumerate(curr_tr_list):

            print(f'transaction {j+1} of {len(curr_tr_list)}: {txid}')
            tx = await get_tx(txid)
            row = {}
            row = tr_features(row, tx['data'])
            row = inp_outp_features(row, tx['data'])
            err, row, new_txs = await time_features(row, tx['data'])
            print(f'errors: {err}')
            row = diff_time_features(row) 
            df[row['txid']] = row
            print()

            next_level_txs.extend(new_txs)
        finished_txs.extend(curr_tr_list)
        curr_tr_list = next_level_txs

    async with aiofiles.open(path_fin, '') as f:
        await f.write(ujson.dumps(df))


async def get_by_address_list(curr_add_list, path_fin, n=1):
    df = {}
    finished_adds = []

    for i in range(n): 
        next_level_adds = []
        print(f'level {i+1} of {n}')

        tasks = [get_tx_by_add(add) for add in curr_add_list]
        chunks_size = 3
        chunks = list(chunkify(tasks, chunks_size))
        print('adds:', len(curr_add_list), 'chunks:', len(chunks))

        for chunk_i, chunk in enumerate(chunks, start=1):
            print(f'[address chunk #{chunk_i}/{len(chunks)}]: fetching...')
            txs_list = await asyncio.gather(*chunk)

            for k, tx in enumerate(txs_list):
                print(f'tx {k+1} of {len(txs_list)}')
                row = {}
                row = tr_features(row, tx)
                row = inp_outp_features(row, tx)
                err, row, new_txs = await time_features(row, tx)
                print(f'errors: {err}')
                row = diff_time_features(row)
                df[row['txid']] = row

                new_inp_adds = [x['prev_address'] for x in row['list_of_inp_adds']]
                new_outp_adds = [x['prev_address'] for x in row['list_of_outp_adds']]
                next_level_adds.extend(new_inp_adds)
                next_level_adds.extend(new_outp_adds)

                print() 

        finished_adds.extend(curr_add_list)
        curr_add_list = next_level_adds

    async with aiofiles.open(path_fin, 'w') as f:
        await f.write(ujson.dumps(df))

    

async def main():
    global curr_tr_list
    global curr_add_list
    global path_tx_fin
    global path_add_fin
    if len(sys.argv) != 2:
        print('Usage:')
        print('  python3 titles.py <command>\n')
        print('Available commands:')
        print(' - get_by_transaction_list')
        print(' - get_by_address_list')
        sys.exit(1)
    if sys.argv[1] == 'get_by_transaction_list':
        await get_by_transaction_list(curr_tr_list=curr_tr_list, path_fin=path_tx_fin)
    elif sys.argv[1] == 'get_by_address_list':
        await get_by_address_list(curr_add_list=curr_add_list, path_fin=path_add_fin)


if __name__ == '__main__':
    asyncio.get_event_loop().run_until_complete(
        main()
    )











