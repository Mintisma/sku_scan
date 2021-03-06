from scrapy.selector import Selector
import requests
import time
from datetime import datetime
import MySQLdb
from multiprocessing import Pool

headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_13_0) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/61.0.3163.100 Safari/537.36'}
s = requests.session()


def get_asins_us():
    # connect
    conn = MySQLdb.connect(host='188.166.226.218', port=3306, user='root', passwd='camry123', db='sales_data',
                           charset="utf8", use_unicode=True)
    cursor = conn.cursor()

    # get data
    inser_sql = """
        select * from product_to_scrape where country = 'us';
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

    url = 'https://www.amazon.com/dp/' + asin
    r = s.get(url, headers=headers)
    selector = Selector(text=r.text)

    def get_rank_small(selector):
        try:
            rank_span = selector.xpath('//th[contains(text(),"Best Sellers Rank")]/parent::tr/td/span/span')
            rank_small = rank_span[1].xpath('text()').extract_first().split()[0].replace('#', '').replace(',', '')
        except Exception as e:
            try:
                rank_span = selector.xpath('//*[contains(text(),"Best Sellers Rank")]/parent::li')
                rank_small = rank_span.xpath('ul/li/span/text()').extract_first().replace('#', '')
            except Exception as e:
                print(e)
                print('rank_small')
                rank_small = 999999
        return rank_small

    def get_onShelf(selector):
        try:
            on_shelf = selector.xpath('//th[contains(text(),"Date First Available")]/parent::tr/td/text()').extract_first(
                '').strip()
            on_shelf = datetime.strptime(on_shelf, '%B %d, %Y').date()
        except Exception as e:
            print(e)
            on_shelf = datetime.today().date()
        return on_shelf

    def get_rank_large(selector):
        try:
            rank_span = selector.xpath('//th[contains(text(),"Best Sellers Rank")]/parent::tr/td/span/span')
            rank_large = rank_span[0].xpath('text()').extract_first().split()[0].replace('#', '').replace(',', '')
        except Exception as e:
            try:
                rank_span = selector.xpath('//*[contains(text(),"Best Sellers Rank")]/parent::li')
                rank_large = rank_span.xpath('text()')[1].extract().strip().split()[0].replace('#', '').replace(',', '')
            except Exception as e:
                print(e)
                print('rank_large')
                rank_large = 1
        return rank_large

    def get_review(selector):
        try:
            review = int(selector.xpath('//span[@id="acrCustomerReviewText"]/text()').extract_first('').split()[0])
        except Exception as e:
            review = 0
        return review

    def get_star(selector):
        try:
            star = float(
                selector.xpath("//span[@id='acrPopover']/span/a/i/span[@class='a-icon-alt']/text()").extract_first(
                    '').split()[0])
        except Exception as e:
            star = 0
        return star

    def get_price(selector):
        try:
            price_1 = float(
                selector.xpath('//span[@id="priceblock_ourprice"]/text()').extract_first('').replace('$', ''))
        except Exception as e:
            price_1 = 999
        try:
            price_2 = float(
                selector.xpath('//span[@id="priceblock_dealprice"]/text()').extract_first('').replace('$', ''))
        except Exception as e:
            price_2 = 999
        try:
            price_3 = float(
                selector.xpath('//span[@id="priceblock_saleprice"]/text()').extract_first('').replace('$', ''))
        except Exception as e:
            price_3 = 999
        price = min(price_1, price_2, price_3)
        return price

    def insert_data(data):
        # 建立连接
        conn = MySQLdb.connect(host='188.166.226.218', port=3306, user='root', passwd='camry123', db='sales_data',
                               charset="utf8", use_unicode=True)
        cursor = conn.cursor()
        insert_sql = """
            insert into us (`asin`, `rank_large`, `rank_small`, `review`, `price`, `star`, `category`, `sku`, `scrape_time`, `store`, `country`) values
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


# asin to be scraped
asins = get_asins_us()
# run code in a schedule
# schedule.every().day.at("16:30").do(multi_info, asins)
# while True:
#     schedule.run_pending()
#     time.sleep(1)

if __name__ == '__main__':
    start = datetime.now()
    multi_info(asins)
    end = datetime.now()
    print('time used: %s' % (end - start))


