import asyncio, aiohttp, random, itertools, sys, json, os
from aiohttp_socks import SocksConnector, SocksVer
from aiohttp_socks.errors import SocksError, SocksConnectionError
from proxy_list import USER_AGENTS, PROXIES


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
FETCH_ATTEMPTS = 5
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

                # отправляем запрос
                response = await session.get(
                    url,
                    params=params,
                    headers={"User-Agent": ua},
                    proxy=proxy_url,
                    timeout=aiohttp.ClientTimeout(total=FETCH_TIMEOUT),
                )
                if response.status != 200:  # получили ошибку
                    data = await response.text()
                    print(f'{response.url} returned HTTP {response.status}: {data}')
                    await asyncio.sleep(1.0)
                    continue  # пробуем еще раз (следующий attempt в цикле)
                else:  # успешно
                    data = await response.json()
                    return data
        except (asyncio.TimeoutError, SocksError, SocksConnectionError, ConnectionResetError, BrokenPipeError, aiohttp.client_exceptions.ClientError) as e:
        # except (ValueError) as e:  # ловлю левое исключение, чтобы отловить ошибки при тесте
            # получили ошибку (при работе через прокси-сервер или еще какую, от aiohttp)
            print(f'exception @ {params}:', e, str(e))
            continue  # пробуем еще раз

    # а тут мы очутимся, если все попытки пройдут безуспешно, тогда функция по умолчанию вернет return None


async def get_transactions_by_address(addr, page=0):
    # ультра-мелкая обертка для удобства
    # return await fetch("https://httpbin.org/json")
    print(f'page={page}: getting transactions...')
    data = await fetch("https://blockexplorer.com/api/txs/", params={"address": addr, "pageNum": page})

    if data and 'txs' in data:
        txs = data['txs']
        pages = data['pagesTotal']
        print(f'page={page}: got {len(txs)} transactions ({pages} pages total).')
    else:
        print(f'page={page}: error :(')
    return data

async def get_all_transactions_by_address(addr):
    # получаем первую страницу
    data = await get_transactions_by_address(addr)
    if not data:
        print("не удалось забрать инфу по этому адресу((( или нет транзакций")
        return
    
    pages = data['pagesTotal']

    all_txs = []  # тут будут все транзакции

    
    all_txs += data['txs']  # запихнем туда транзакции из первой страницы, которые скачали ранее

    # делаем список из корутин (запланированных для дальнейшего выполнения параллельно функций)
    # для скачивания страниц со второй по последнюю
    tasks = [get_transactions_by_address(addr, page=page) for page in range(1, pages)]

    # брать их все одновременно, если там 40+ страниц - так себе идея
    # разобьем на куски по 8
    chunks = chunkify(tasks, 8)

    for chunk in chunks:
        # отправляем одновременно 8 запросов по странице
        chunk_results = await asyncio.gather(*chunk)

        for result in chunk_results:
            if not result:  # упс, при попытке получить данные по какой-то странице вернулась ошибочка
                print(f"Got no data (out of attempts/timeout/etc) :(")
                continue  # пропускаем эту страницу нафиг. [TODO]: пробовать еще раз до победного конца
            all_txs += result['txs']

    return all_txs


def main():
    if len(sys.argv) == 2:
        addr = sys.argv[1]
    else:
        # addr = '3H23DyudPTpyK1dEj6vxzUpuczFcue7YKR'
        print('Usage: python aioblockexplorer.py <address>')
        return
    
    filename = f'transactions_{addr}.json'

    if os.path.exists(filename):
        print(f"{filename} already exists. Skipping...")
        return

    data = asyncio.get_event_loop().run_until_complete(
        get_all_transactions_by_address(addr)
    )

    # # отображаем результат выполнения get_all_transactions_by_address
    # print(data)

    # сохраняем в json файлик, если есть что сохранять ваще
    if data:
        with open(filename, 'w') as f:
            json.dump(data, f)

        print(f"Exported to {filename}")


if __name__ == '__main__':
    main()
