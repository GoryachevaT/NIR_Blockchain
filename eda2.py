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
import re
import math


CACHE_DIR_TX = '/tmp/btc_txids'
CACHE_DIR_ADDS = '/tmp/btc_adds'
os.makedirs(CACHE_DIR_TX, exist_ok=True)
os.makedirs(CACHE_DIR_ADDS, exist_ok=True)

curr_tr_list = tr_list
curr_add_list = addr_list
path_tx_fin = './tx_json.json'
path_add_fin = './add_jsons'


async def get_tx(txid, attempts=5):
    #print('get_tx:', txid)
    path = f'{CACHE_DIR_TX}/{txid}.json'
    if os.path.exists(path):
        #print(f'{txid} exists')
        for attempt in range(attempts):
           try:
                try:
                    async with aiofiles.open(path, 'r') as f:
                        data = ujson.loads(await f.read())
                    return data
                except (ValueError) as e:
                    return {"data": {"hash": txid}, "err_no": 666}
           except (OSError) as e:
               print(f'exception:', e, str(e))
               continue  # пробуем еще раз
    else:
        for attempt in range(attempts):
            #print(f'{txid} is new')
            data = await fetch(f'https://chain.api.btc.com/v3/tx/{txid}')
            if not data or data.get('err_no', 0) == 2:
                #print(f'{txid}: {data}')
                continue
            else: 
                async with aiofiles.open(path, 'w') as f:
                    await f.write(ujson.dumps(data))
                return data
        
    if data is None or not data:
        return {"data": {"hash": txid}, "err_no": 666}

    return data


async def get_transactions_by_address(addr, page=1):
    data = await fetch(f'https://chain.api.btc.com/v3/address/{addr}/tx?page={page}')
    if data and 'data' in data:
        txs = data['data']['list']
        total_count = data['data']['total_count']
        pagesize = 50
        pages = math.ceil(total_count/pagesize)
        return data['data']
    else:
        return {"data": {"hash": None}, "err_no": 666}



async def get_all_transactions_by_address(addr):
    print(f'addr {addr}')
    data = await get_transactions_by_address(addr)
    if not data:
        print("не удалось забрать инфу по этому адресу((( или нет транзакций")
        return
    total_count = data['total_count']
    pagesize = 50
    pages = math.ceil(total_count/pagesize)
    all_txs = []
    all_txs += data['list']
    tasks = [get_transactions_by_address(addr, page=page) for page in range(2, pages+1)]
    chunks = chunkify(tasks, 8)
    for chunk in chunks:
        chunk_results = await asyncio.gather(*chunk)

        for result in chunk_results:
            if not result:  
                print(f"Got no data (out of attempts/timeout/etc) :(")
                continue  
            all_txs += result['list']
    return all_txs


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

async def create_time_features(err, row, addr_list, addr_type, attempts=3):

    txs = []
    times = []
    vals = []
    meta_tr_dict = []

    if addr_type == 'input': 
        field_type = 'inp_address'
        add_type = 'prev_addresses'
        value_type = 'prev_value'
        hash_type = 'prev_tx_hash'
        meta_addr_field = 'prev_address'
        meta_time_field = 'prev_time'
        meta_value_field = 'total_prev_val'
        meta_agg_value_field = 'prev_val'
    elif addr_type == 'output':
        field_type = 'outp_address'
        add_type = 'addresses'
        value_type = 'value'
        hash_type = 'spent_by_tx'
        meta_addr_field = 'next_address'
        meta_time_field = 'next_time'
        meta_value_field = 'total_next_val'
        meta_agg_value_field = 'next_val'

    meta_txs_tasks = [get_tx(itx[hash_type]) for itx in addr_list]
    addresses = [{field_type: itx[add_type][0], 'curr_value': itx[value_type]} for itx in addr_list]
    chunks_size = 16
    txs_chunks = list(chunkify(meta_txs_tasks, chunks_size))
    address_chunks = list(chunkify(addresses, chunks_size))

         
    #print(f'addresses: {len(addresses)}, meta_txs_tasks: {len(meta_txs_tasks)}')
    for chunk_i, (txs_chunk, address_chunk) in enumerate( list(zip(txs_chunks,address_chunks)), start=1):
        #print(f'[chunk #{chunk_i}/{len(chunks)}]: fetching...')
        tmp_txs = await asyncio.gather(*txs_chunk)
        for j, tmp_tx in enumerate(tmp_txs):
            

            try:
                txid = tmp_tx['data']['hash']
                txs.append(txid)
                if tmp_tx.get('err_no', '') != 0:
                    err.append(txid)
                    time = None
                    val = None
                else:
                    time = tmp_tx['data']['block_time']
                    val = tmp_tx['data']['inputs_value']
            
                #print(f'time = {time}')
                #print(f'value = {val}')
                times.append(time)
                vals.append(val)
                meta_tr_dict.append({meta_addr_field: address_chunk[j][field_type],
                                     'curr_value': address_chunk[j]['curr_value'],
                                     meta_time_field: time,
                                     meta_value_field: val
                                     })
            except TypeError as e:  
                #print(f'exception:', e, str(e))
                #print(tmp_tx)
                #print(attempt)

                continue  # пробуем еще раз


    times = [x for x in times if x]

    if times:
        row[f'max_{meta_time_field}'] = max(times)
        row[f'min_{meta_time_field}'] = min(times)
        row[f'avg_{meta_time_field}'] = round(mean(times))
    else: 
        row[f'max_{meta_time_field}'] = None
        row[f'min_{meta_time_field}'] = None
        row[f'avg_{meta_time_field}'] = None

    vals = [x for x in vals if x]
    if vals:
        row[f'max_{meta_agg_value_field}'] = max(vals)
        row[f'min_{meta_agg_value_field}'] = min(vals)
        row[f'avg_{meta_agg_value_field}'] = round(mean(vals))
    else: 
        row[f'max_{meta_agg_value_field}'] = None
        row[f'min_{meta_agg_value_field}'] = None
        row[f'avg_{meta_agg_value_field}'] = None
    row[f'list_of_{addr_type}'] = meta_tr_dict

    return err, row, txs

 
async def time_features(row, tx):
    new_txs = []
    err = []
    err, row, txs = await create_time_features(err=err, row=row, addr_list=tx['inputs'], addr_type='input')
    new_txs += txs
    err, row, txs = await create_time_features(err=err, row=row, addr_list=tx['outputs'], addr_type='output')
    new_txs += txs
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


async def create_row(tx):
    print(tx['hash'])
    row = {}
    row = tr_features(row, tx)
    row = inp_outp_features(row, tx)
    err, row, new_txs = await time_features(row, tx)
    #print(f'errors: {err}')
    row = diff_time_features(row)
    new_inp_adds = [x['prev_address'] for x in row['list_of_input']]
    new_outp_adds = [x['next_address'] for x in row['list_of_output']]
    return row, new_txs, new_inp_adds, new_outp_adds


async def get_info_by_transaction_list(curr_tr_list, path_fin, n=3):
    df = {}
    finished_txs = []
    for i in range(n): 
        next_level_txs = []
        print()
        print('\n===========================================================================================================\
               \n===========================================================================================================')


        tx_tasks = [get_tx(tx) for tx in curr_tr_list]        
        tx_chunks_size = 16
        tx_chunks = list(chunkify(tx_tasks, tx_chunks_size))
        print('transactions:', len(curr_tr_list), 'chunks:', len(tx_chunks))
        for chunk_i, tx_chunk in enumerate(tx_chunks, start=1):
            
            print('\n==================================================================================')
            print(f'[tx chunk #{chunk_i}/{len(tx_chunks)}]: fetching...')
            data = await asyncio.gather(*tx_chunk)


            create_row_tasks = [create_row(tx['data']) for tx in data]        
            create_row_chunks_size = 8
            create_row_chunks = list(chunkify(create_row_tasks, create_row_chunks_size))
            print('txs to create new rows:', len(data), 'chunks:', len(create_row_chunks))
            for chunk_j, create_row_chunk in enumerate(create_row_chunks, start=1):
                
                print('\n===========================================')
                print(f'[create_row chunk #{chunk_j}/{len(create_row_chunks)}]: fetching...')
                new_rows = await asyncio.gather(*create_row_chunk)

                for step in new_rows:
                    row, new_txs, new_inp_adds, new_outp_adds = step[0], step[1], step[2], step[3]
                    df[row['txid']] = row
                    next_level_txs += new_txs

            # for tx in data:
            #     row, new_txs, new_inp_adds, new_outp_adds = await create_row(tx['data'])
            #     df[row['txid']] = row
            #     print()
            #     next_level_txs += new_txs

        finished_txs.extend(curr_tr_list)
        curr_tr_list = next_level_txs



        # for j, txid in enumerate(curr_tr_list):
        #     print(f'transaction {j+1} of {len(curr_tr_list)}: {txid}')
        #     tx = await get_tx(txid)
        #     row, new_txs, new_inp_adds, new_outp_adds = await create_row(tx['data'])
        #     df[row['txid']] = row
        #     print()
        #     next_level_txs.extend(new_txs)
        # finished_txs.extend(curr_tr_list)
        # curr_tr_list = next_level_txs

    async with aiofiles.open(path_fin, 'a') as f:
        await f.write(ujson.dumps(df))


async def get_info_by_address_list(curr_add_list, path_fin, n=1):

    with open('./finished_adds', 'r') as f:
        finished_adds = f.read()

    with open('./1_level_adds', 'r') as f:
        curr_add_list_read = f.read()

    curr_add_list_read = re.sub('[\[\]\"]', '', curr_add_list_read).split(',')
    finished_adds = re.sub('[\[\]\"]', '', finished_adds).split(',')

    #finished_adds = []
    add_errors = []

    curr_add_list_new = [add for add in curr_add_list_read if add not in finished_adds]

    for i in range(n): 
        next_level_adds = []
        print('\n===========================================================================================================\
               \n===========================================================================================================')
        print(f'level {i+1} of {n}')


        add_tasks = [get_all_transactions_by_address(add) for add in curr_add_list_new]    
        addresses = [add for add in curr_add_list_new]     
        add_chunks_size = 5
        add_info_chunks = list(chunkify(add_tasks, add_chunks_size))
        add_list_chunks = list(chunkify(addresses, add_chunks_size))
        print('adds:', len(curr_add_list_new), 'chunks:', len(add_info_chunks))
        for chunk_i, add_chunk in enumerate(add_info_chunks, start=1):
            
            print('\n==================================================================================\
                   \n==================================================================================')
            print(f'[add chunk #{chunk_i}/{len(add_info_chunks)}]: fetching...')
            data = await asyncio.gather(*add_chunk)

            print(f'number of recieved txs:')
            for add in data:
                print(f'add1: {len(add)}')
            flat_data = [item for sublist in data for item in sublist]

            create_row_tasks = [create_row(tx) for tx in flat_data]        
            row_chunks_size = 5
            row_chunks = list(chunkify(create_row_tasks, row_chunks_size))
            print('rows:', len(flat_data), 'chunks:', len(row_chunks))
            for chunk_j, row_chunk in enumerate(row_chunks, start=1):
                print(f'[row chunk #{chunk_j}/{len(row_chunks)}]: fetching...')
                rows = await asyncio.gather(*row_chunk)
                for new_row in rows:
                    row, new_txs, new_inp_adds, new_outp_adds = new_row[0], new_row[1], new_row[2], new_row[3]
                    next_level_adds += new_inp_adds
                    next_level_adds += new_outp_adds
                    txid = row['txid']
                    path = path_fin+f'/{txid}.json'
                    if os.path.exists(path) == False:
                        async with aiofiles.open(path, 'w') as f:
                             await f.write(ujson.dumps(row))

            finished_adds.extend(add_list_chunks[chunk_i-1])

            async with aiofiles.open('./finished_adds', 'w') as f:
                await f.write(ujson.dumps(finished_adds))

        async with aiofiles.open(f'./{i+2}_level_adds', 'w') as f:
            await f.write(ujson.dumps(next_level_adds))

        curr_add_list_new = [add for add in next_level_adds if add not in finished_adds]

    

async def main():
    global curr_tr_list
    global curr_add_list
    global path_tx_fin
    global path_add_fin
    if len(sys.argv) != 2:
        print('Usage:')
        print('  python3 titles.py <command>\n')
        print('Available commands:')
        print(' - get_info_by_transaction_list')
        print(' - get_info_by_address_list')
        sys.exit(1)
    if sys.argv[1] == 'get_info_by_transaction_list':
        await get_info_by_transaction_list(curr_tr_list=curr_tr_list, path_fin=path_tx_fin)
    elif sys.argv[1] == 'get_info_by_address_list':
        await get_info_by_address_list(curr_add_list=curr_add_list, path_fin=path_add_fin)


if __name__ == '__main__':
    asyncio.get_event_loop().run_until_complete(
        main()
    )











