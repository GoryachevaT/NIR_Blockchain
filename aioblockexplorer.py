import asyncio, aiohttp, random, itertools, sys, json, os
from aiohttp_socks import SocksConnector, SocksVer
from aiohttp_socks.errors import SocksError, SocksConnectionError



USER_AGENTS = [
    "Mozilla/5.0 (X11; Linux i686; rv:64.0) Gecko/20100101 Firefox/64.0",
    "Mozilla/5.0 (Windows NT 6.1; WOW64; rv:64.0) Gecko/20100101 Firefox/64.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.14; rv:10.0) Gecko/20100101 Firefox/62.0",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/71.0.3578.98 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/64.0.3282.140 Safari/537.36 Edge/17.17134",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:64.0) Gecko/20100101 Firefox/64.0",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/72.0.3626.109 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/71.0.3578.98 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/73.0.3683.75 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:65.0) Gecko/20100101 Firefox/65.0",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/71.0.3578.98 Safari/537.36 OPR/58.0.3135.107",
    "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:65.0) Gecko/20100101 Firefox/65.0",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/71.0.3578.98 Safari/537.36 OPR/58.0.3135.107",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/73.0.3683.75 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/72.0.3626.122 Safari/537.36 Vivaldi/2.3.1440.61",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/73.0.3683.75 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/73.0.3683.75 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/73.0.3683.75 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/73.0.3683.75 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/73.0.3683.75 Safari/537.36",
]


PROXIES = {
    'HTTP': '''
113.254.114.24:8197
170.82.228.42:8080
221.2.175.238:8060
82.179.156.239:8080
181.225.109.133:8181
41.162.39.189:30617
221.1.205.74:8060
117.191.11.79:8080
91.210.178.161:8080
41.87.29.130:8080
120.132.52.218:8888
120.132.53.5:8888
169.239.45.37:8080
188.233.188.128:31244
    ''',
    
    'SOCKS': '''
88.218.51.222:8000:kvMbut:oMaKT4
77.83.185.85:8000:mLK68g:t3BgUt
77.83.185.55:8000:mLK68g:t3BgUt
77.83.185.138:8000:mLK68g:t3BgUt
77.83.184.83:8000:mLK68g:t3BgUt
77.83.187.229:8000:mLK68g:t3BgUt
77.83.186.101:8000:mLK68g:t3BgUt
77.83.185.52:8000:mLK68g:t3BgUt
77.83.184.52:8000:mLK68g:t3BgUt
77.83.187.21:8000:mLK68g:t3BgUt
77.83.187.184:8000:mLK68g:t3BgUt
77.83.184.229:8000:mLK68g:t3BgUt
77.83.184.132:8000:mLK68g:t3BgUt
77.83.185.169:8000:mLK68g:t3BgUt
77.83.184.222:8000:mLK68g:t3BgUt
77.83.184.113:8000:mLK68g:t3BgUt
77.83.187.199:8000:mLK68g:t3BgUt
77.83.187.107:8000:mLK68g:t3BgUt
77.83.187.6:8000:mLK68g:t3BgUt
77.83.28.132:8000:VjP3pB:pFe4Wy
185.99.98.45:8000:eNZQtc:4BoGZr
185.240.49.71:8000:d4vgev:CVY7ZQ
185.202.108.224:8000:hyV1Yf:tZSTBq
195.158.194.41:8000:4K9oLs:PPmNtV
185.196.61.83:8000:kWozwT:q4F8p5
88.218.51.58:8000:Svfzov:M4WASx
196.19.120.158:8000:3UMrZp:nGCWC3
185.36.190.187:8000:evmRJX:xxJjEh
196.18.165.233:8000:Gr3pDb:390WWq
185.240.48.150:8000:mzaYBt:GGwfZ8
196.17.168.29:8000:TaX4Cv:5CdpsA
185.202.2.184:8000:nrpkm6:ejcX1j
185.202.2.120:8000:fcJsNg:rWcZgb
185.196.61.50:8000:xAXCHz:SoBEq2
195.158.194.52:8000:bsZoKs:KFCCQn
196.17.223.184:8000:Ht5whH:xgQM98
196.16.114.57:8000:0XF8fS:28RuWd
196.19.243.3:8000:7oWa3r:uy7F0C
196.18.227.97:8000:7oWa3r:uy7F0C
185.99.98.55:8000:v9kreS:uvE4ne
185.126.67.186:8000:v9kreS:uvE4ne
196.16.114.124:8000:bL0SW7:PUPBFM
196.16.115.236:8000:bL0SW7:PUPBFM
193.58.109.159:8000:zUMv2x:dPzZ6x
196.16.113.107:8000:eQj4hq:RsPMRp
213.139.218.167:8000:4VhscX:H10JQr
185.240.49.41:8000:TMpaex:5V0MZF
196.18.14.152:8000:c8FSe9:UT7rYm
213.139.219.85:8000:5YfW9b:gTNDGP
185.196.63.146:8000:CXWKh8:6NNZAs
196.16.113.186:8000:9exFTt:FKunAV
193.58.109.152:8000:W7mVZK:3BQ9sb
185.167.162.155:8000:bgqkNY:YmAXmM
    ''',
}

PROXIES['HTTP'] = [tuple(line.split(':')) for line in PROXIES['HTTP'].strip().split('\n')]
PROXIES['SOCKS'] = [tuple(line.split(':')) for line in PROXIES['SOCKS'].strip().split('\n')]

# перемешиваем список проксей для равномерного распределения
random.shuffle(PROXIES['HTTP'])
random.shuffle(PROXIES['SOCKS'])

# создаем итератор, который будет возвращать при вызове next(PROXY_ITER) каждый раз разный прокси
PROXY_ITER = {
    'HTTP': itertools.cycle(PROXIES['HTTP']),
    'SOCKS': itertools.cycle(PROXIES['SOCKS']),
}


USE_PROXY = 'SOCKS'  # False, 'HTTP', 'SOCKS'
FETCH_ATTEMPTS = 3
FETCH_TIMEOUT = 10


def chunkify(seq, size):
    return (seq[pos:pos + size] for pos in range(0, len(seq), size))


def get_random_proxy_params(use_proxy):
    if not use_proxy:
        connector = None
        proxy_url = None
    elif use_proxy == 'HTTP':
        connector = None
        proxy = next(PROXY_ITER['HTTP'])
        if len(proxy) == 2:
            host, port = proxy
            proxy_url = f'http://{host}:{port}'
        elif len(proxy) == 4:
            host, port, username, password = proxy
            proxy_url = f'http://{username}:{password}@{host}:{port}'
    elif use_proxy == 'SOCKS':
        proxy = next(PROXY_ITER['SOCKS'])
        if len(proxy) == 2:
            host, port = proxy
            username = password = None
        elif len(proxy) == 4:
            host, port, username, password = proxy
        connector = SocksConnector(SocksVer.SOCKS5, host, int(port), username=username, password=password)
        proxy_url = None
    return connector, proxy_url


async def fetch(url, params=None, attempts=FETCH_ATTEMPTS, use_proxy=USE_PROXY):
    """ обёртка над aiohttp.get() с повтором запроса при ошибке + работа с прокси """
    for attempt in range(attempts):
        try:
            connector, proxy_url = get_random_proxy_params(use_proxy)

            async with aiohttp.ClientSession(connector=connector) as session:
                ua = random.choice(USER_AGENTS)

                # собсна, отправляем запрос
                response = await session.get(
                    url,
                    params=params,
                    headers={"User-Agent": ua},
                    proxy=proxy_url,
                    timeout=aiohttp.ClientTimeout(total=FETCH_TIMEOUT),
                )
                if response.status != 200:  # получили ошибку
                    # print('proxy:', host, 'ua:', ua)
                    data = await response.text()
                    print(f'{response.url} returned HTTP {response.status}: {data}')
                    await asyncio.sleep(1.0)
                    continue  # пробуем еще раз (следующий attempt в цикле)
                else:  # всё ок
                    data = await response.json()
                    return data
        except (asyncio.TimeoutError, SocksError, SocksConnectionError, aiohttp.client_exceptions.ClientError) as e:
        # except (ValueError) as e:  # ловлю левое исключение, чтобы отловить ошибки при тесте
            # получили ошибку (при работе через прокси-сервер или еще какую, от aiohttp)
            print(f'exception @ {params}:', e, str(e))
            continue  # пробуем еще раз

    # а тут мы очутимся, если все попытки пройдут безуспешно, тогда функция по умолчанию вернет return None


# async def get_transactions_by_address(addr, page=0):
#     # ультра-мелкая обертка для удобства
#     # return await fetch("https://httpbin.org/json")
#     print(f'page={page}: getting transactions...')
#     data = await fetch("https://blockexplorer.com/api/txs/", params={"address": addr, "pageNum": page})

#     if data and 'txs' in data:
#         txs = data['txs']
#         pages = data['pagesTotal']
#         print(f'page={page}: got {len(txs)} transactions ({pages} pages total).')
#     else:
#         print(f'page={page}: error :(')
#     return data

# async def get_all_transactions_by_address(addr):
#     # получаем первую страницу
#     data = await get_transactions_by_address(addr)
#     if not data:
#         print("не удалось забрать инфу по этому адресу((( или нет транзакций")
#         return
    
#     pages = data['pagesTotal']

#     all_txs = []  # тут будут все транзакции

#     
#     all_txs += data['txs']  # запихнем туда транзакции из первой страницы, которые скачали ранее

#     # делаем список из корутин (запланированных для дальнейшего выполнения параллельно функций)
#     # для скачивания страниц со второй по последнюю
#     tasks = [get_transactions_by_address(addr, page=page) for page in range(1, pages)]

#     # брать их все одновременно, если там 40+ страниц - так себе идея
#     # разобьем на куски по 8
#     chunks = chunkify(tasks, 8)

#     for chunk in chunks:
#         # отправляем одновременно 8 запросов по странице
#         chunk_results = await asyncio.gather(*chunk)

#         for result in chunk_results:
#             if not result:  # упс, при попытке получить данные по какой-то странице вернулась ошибочка
#                 print(f"Got no data (out of attempts/timeout/etc) :(")
#                 continue  # пропускаем эту страницу нафиг. [TODO]: пробовать еще раз до победного конца
#             all_txs += result['txs']

#     return all_txs


# def main():
#     if len(sys.argv) == 2:
#         addr = sys.argv[1]
#     else:
#         # addr = '3H23DyudPTpyK1dEj6vxzUpuczFcue7YKR'
#         print('Usage: python aioblockexplorer.py <address>')
#         return
    
#     filename = f'transactions_{addr}.json'

#     if os.path.exists(filename):
#         print(f"{filename} already exists. Skipping...")
#         return

#     data = asyncio.get_event_loop().run_until_complete(
#         get_all_transactions_by_address(addr)
#     )

#     # # отображаем результат выполнения get_all_transactions_by_address
#     # print(data)

#     # сохраняем в json файлик, если есть что сохранять ваще
#     if data:
#         with open(filename, 'w') as f:
#             json.dump(data, f)

#         print(f"Exported to {filename}")


# if __name__ == '__main__':
#     main()
