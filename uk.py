from scrapy.selector import Selector
import requests
import time
from datetime import datetime
import MySQLdb
from multiprocessing import Pool

headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_13_0) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/61.0.3163.100 Safari/537.36'}
s = requests.session()


def get_asins_uk():
    # connect
    conn = MySQLdb.connect(host='188.166.226.218', port=3306, user='root', passwd='camry123', db='sales_data',
                           charset="utf8", use_unicode=True)
    cursor = conn.cursor()

    # get data
    inser_sql = """
        select * from product_to_scrape where country = 'uk';
    """
    cursor.execute(inser_sql)
    data = cursor.fetchall()

    # close connection
    cursor.close()
    conn.close()
    return data


def get_info(asin):
    time.sleep(1)

    country = asin[4]
    sku = asin[0]
    category = asin[3]
    store = asin[2]
    asin = asin[1]

    url = 'https://www.amazon.co.uk/dp/' + asin
    r = s.get(url, headers=headers)
    selector = Selector(text=r.text)

    def get_rank_large(selector):
        try:
            rank_span = selector.xpath('//td[contains(text(),"Best Sellers Rank")]/parent::tr/td')
            rank_large = rank_span[1].xpath('text()').extract_first().strip().split()[0].replace(',', '').replace('#','')
        except Exception as e:
            print(e)
            rank_large = 1
        return rank_large

    def get_rank_small(selector):
        try:
            rank_span = selector.xpath('//td[contains(text(),"Best Sellers Rank")]/parent::tr/td')
            rank_small = rank_span[1].xpath('ul/li/span/text()').extract_first().strip().split()[0].replace('#','').replace(',', '')
        except Exception as e:
            print(e)
            rank_small = 999999
        return rank_small

    def get_review(selector):
        try:
            review = int(selector.xpath('//span[@id="acrCustomerReviewText"]/text()').extract_first('').split()[0])
        except Exception as e:
            review = 0
        return review

    def get_star(selector):
        try:
            star = float(selector.xpath("//span[@id='acrPopover']/span/a/i/span[@class='a-icon-alt']/text()").extract_first('').split()[0])
        except Exception as e:
            star = 0
        return star

    def get_price(selector):
        try:
            price = float(selector.xpath('//span[@id="priceblock_saleprice"]/text()').extract_first('').replace('£', ''))
        except Exception as e:
            try:
                price = float(
                    selector.xpath('//span[@id="priceblock_ourprice"]/text()').extract_first('').replace('£', ''))
            except Exception as e:
                price = 999
        return price

    def insert_data(data):
        # 建立连接
        conn = MySQLdb.connect(host='188.166.226.218', port=3306, user='root', passwd='camry123', db='sales_data',
                               charset="utf8", use_unicode=True)
        cursor = conn.cursor()
        insert_sql = """
            insert into eu (`asin`, `rank_large`, `rank_small`, `review`, `price`, `star`, `category`, `sku`, `scrape_time`, `store`, `country`) values
            (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        params = (data['asin'], data['rank_large'], data['rank_small'], data['review'], data['price'], data['star'],
                  data['category'], data['sku'], data['scrape_time'], data['store'], data['country'])
        try:
            cursor.execute(insert_sql, params)
            conn.commit()
        except Exception as e:
            print(e)
            print('insert_data')
        finally:
            cursor.close()
            conn.close()

    data = {
        'asin': asin,
        'rank_large': get_rank_large(selector),
        'rank_small': get_rank_small(selector),
        'review': get_review(selector),
        'price': get_price(selector),
        'star': get_star(selector),
        'scrape_time': datetime.now().date(),
        'store': store,
        'category': category,
        'sku': sku,
        'country': country,
    }

    insert_data(data)


def multi_info(asins):
    # 建立进程池
    pool = Pool()
    temp = []
    for asin in asins:
        temp.append(pool.apply_async(get_info, args=(asin,)))
    pool.close()
    pool.join()

    # 执行多进程
    for item in temp:
        item.get()


asins = get_asins_uk()

if __name__ == '__main__':
    start = datetime.now()
    multi_info(asins)
    end = datetime.now()
    print('time used: %s' % (end - start))

