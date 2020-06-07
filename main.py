import email
import json
import logging
import os
from pathlib import Path
from contextlib import contextmanager, closing
from datetime import datetime

from lxml import html
import requests


log = logging.getLogger(__name__)


@contextmanager
def context(verbose=True, **kwargs):
    kwargs_str = ' '.join(map(lambda i: f'{i[0]}={i[1]}', kwargs.items()))
    if verbose:
        log.info(f'Processing {kwargs_str}')
    try:
        yield None
        if verbose:
            log.info(f'Finished processing {kwargs_str}')
    except Exception as e:
        log.exception(f'Exception while processing {kwargs_str}')


def save(name, response):
    with open(name, 'w', encoding=response.encoding) as f:
        f.write(response.text)


def get(url, headers=None, shop=None):
    response = requests.get(url, headers=headers)
    response.raise_for_status()
    return response


def parse(response):
    return html.fromstring(response.content, parser=html.HTMLParser(encoding='utf-8'))


def process(on_result, shop):
    shop_ids = dict(metro=7, prisma=14, lenta=36)
    shop_id = shop_ids[shop]
    response = get(f'https://igooods.ru/api/v8/shops/{shop_id}/categories')
    categories = [cat['id'] for cat in response.json()['data']['list']]
    log.info(f'Processing categories {" ".join(map(str, categories))}')
    for cat in sorted(categories):
        with context(shop=shop, cat=cat):
            url = 'https://igooods.ru/api/v8/shops/{shop_id}/categories/{cat}/products?sort=price_per_kg_vl&sort_order=asc&offset={offset}'
            data = get(url.format(shop_id=shop_id, cat=cat, offset=0)).json()
            for offset in range(0, data['data']['total'], 30):
                #if offset == 60: return
                data = get(url=url.format(shop_id=shop_id, cat=cat, offset=offset)).json()

                for product in data['data']['list']:
                    with context(verbose=True, shop=shop, cat=cat, offset=offset, product=product):
                        del product['image']
                        product['shop'] = shop
                        product['cat'] = cat
                        # product['url'] = f'https://igooods.ru/api/v8/product?model_id={product["model_id"]}&shop_id={shop_id}'
                        on_result.send(product)


def result_writer(file_name):
    temp_file_name = file_name.with_suffix('.tmp')
    log.info(f'Started writing results to {temp_file_name}')
    try:
        with open(temp_file_name, 'w', encoding='utf-8') as fp:
            while True:
                data = yield
                # log.debug(' '.join(map(lambda i: f'{i[0]}:{i[1]}', data.items())))
                data['datetm'] = str(datetime.now())
                fp.write('{}\n'.format(json.dumps(data, ensure_ascii=False)))
                fp.flush()
    except GeneratorExit:
        log.info(f'Finished writing results. Renaming {temp_file_name} to {file_name}')
        os.rename(temp_file_name, file_name)


def now_str():
    return datetime.now().strftime('%y_%m_%d__%H_%M_%S')


if __name__ == '__main__':
    jsons_path = Path('jsons')
    jsons_path.mkdir(parents=True, exist_ok=True)
    Path('logs').mkdir(parents=True, exist_ok=True)
    handler = logging.StreamHandler()
    handler.setLevel(logging.INFO)
    logging.basicConfig(level=logging.DEBUG,
                        format='%(asctime)s.%(msecs)03d|%(levelname)-4.4s|%(thread)-6.6s|%(funcName)-10.10s|%(message)s',
                        handlers=[logging.FileHandler('logs/food_{}.log'.format(now_str())),
                                  handler])
    logging.getLogger('requests').setLevel(logging.INFO)
    logging.getLogger('urllib3').setLevel(logging.INFO)

    log.info('Started')
    with closing(result_writer(file_name=jsons_path / '{}.json'.format(now_str()))) as on_result:
        on_result.send(None)
        for shop in ['metro', 'prisma', 'lenta']:
            with context(shop=shop):
                process(on_result, shop)
    log.info('Finished')
