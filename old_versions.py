# async def get_page(add, page):
#     path = f'{CACHE_DIR_ADDS}/{add}'
#     os.makedirs(path, exist_ok=True)
#     data = await fetch(f'https://chain.api.btc.com/v3/address/{add}/tx?page={page}')
#     async with aiofiles.open(path+f'/page_{page}.json', 'w') as f:
#             await f.write(ujson.dumps(data))
#     return data['data']['list']


async def get_tx_by_add(add):
    path = f'{CACHE_DIR_ADDS}/{add}'
    data = []

    if os.path.exists(path):
        files = os.listdir(path)
        for file in files:
            with open(path+f'/{file}', 'r') as f:
                tmp = ujson.loads(f.read())
            if tmp is None or not tmp:
                return {"add": add, "err_no": 666}
            else: data.extend(tmp['data']['list'])
    else:
        os.makedirs(path, exist_ok=True)
        tmp = await fetch(f'https://chain.api.btc.com/v3/address/{add}/tx?page=1')
        async with aiofiles.open(path+'/page_1.json', 'w') as f:
            await f.write(ujson.dumps(tmp))

        
        total_count = tmp['data']['total_count']
        pagesize = 50
        pages = math.ceil(total_count/pagesize)
        
        print(f'add {add}, total_count: {total_count}, pages: {pages}')
        data.extend(tmp['data']['list'])

        tasks = [get_page(add, page) for page in range(2, pages+1)]
        chunks_size = 20
        chunks = list(chunkify(tasks, chunks_size))
        for chunk_i, chunk in enumerate(chunks, start=1):
            tmp = await asyncio.gather(*chunk)
            for tx in tmp:
                data += tmp

    print(f'recieved from add {add}: {len(data)} txs')
    return data


async def create_time_features(add_list, type):

    if type == 'input': 
        add_type = 'inp_address'
        value_type = 'prev_value'
        hash_type = 'prev_tx_hash'

    tasks = [get_tx(itx[hash_type]) for itx in add_list]
    prev_addresses = [{add_type: itx['prev_addresses'][0], 'curr_value': itx[value_type]} for itx in add_list]
    chunks_size = 16
    chunks = list(chunkify(tasks, chunks_size))


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
    #print('inputs:', len(tx['inputs']), 'chunks:', len(chunks))
    for chunk_i, chunk in enumerate(chunks, start=1):
        #print(f'[chunk #{chunk_i}/{len(chunks)}]: fetching...')
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
        #print(f'[chunk #{chunk_i}/{len(chunks)}]: done!')

    prev_times = [x for x in prev_times if x]
    if prev_times:
        row['max_prev_time'] = max(prev_times)
        row['min_prev_time'] = min(prev_times)
        row['avg_prev_time'] = round(mean(prev_times))
    else: 
        row['max_prev_time'] = None
        row['min_prev_time'] = None
        row['avg_prev_time'] = None
    #print(f'prev_time fin')
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
    #print('outputs:', len(tx['outputs']), 'chunks:', len(chunks))
    for chunk_i, chunk in enumerate(chunks, start=1):
        #print(f'[chunk #{chunk_i}/{len(chunks)}]: fetching...')
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
        #print(f'[chunk #{chunk_i}/{len(chunks)}]: done!')

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