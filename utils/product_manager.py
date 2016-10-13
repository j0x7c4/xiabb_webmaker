# encoding=utf-8
import MySQLdb
import json
import sys
from settings import mysql_conf, log_conf
import hashlib
import logging
logging.basicConfig(filename=log_conf["filename"],format=log_conf["format"],level=log_conf["level"])

max_cnt = 100

table_product = 'xiabb_product'
table_category = 'xiabb_category'
table_product_img = 'xiabb_product_img'
table_product_price = 'xiabb_product_price'

create_xiabb_product_table_sql = '''
CREATE TABLE IF NOT EXISTS `xiabb_product` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `name` varchar(255) NOT NULL,
  `en_name` varchar(255) NOT NULL,
  `brand` varchar(255) NOT NULL,
  `info` varchar(255) DEFAULT NULL,
  `category` varchar(255) DEFAULT NULL,
  `url` varchar(255) DEFAULT NULL,
  `remote_url` varchar(255) DEFAULT NULL,
  `add_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `update_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  UNIQUE KEY `UK_RAW` (`name`,`brand`,`category`),
  KEY `ADDTIME` (`add_time`),
  KEY `UPDATETIME` (`update_time`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
'''

create_xiabb_product_img_table_sql = '''
CREATE TABLE IF NOT EXISTS `xiabb_product_img` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `product` varchar(255) DEFAULT NULL,
  `brand` varchar(255) DEFAULT NULL,
  `type` int(11) NOT NULL,
  `url` varchar(255) DEFAULT NULL,
  `add_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `update_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  UNIQUE KEY `UK_RAW` (`type`, `url`),
  KEY `ADDTIME` (`add_time`),
  KEY `UPDATETIME` (`update_time`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
'''

create_xiabb_product_price_table_sql = '''
CREATE TABLE IF NOT EXISTS `xiabb_product_price` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `product` varchar(255) NOT NULL,
  `brand` varchar(255) NOT NULL,
  `price` float NOT NULL,
  `ts` varchar(255) NOT NULL,
  `add_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `update_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  UNIQUE KEY `UK_RAW` (`product`,`brand`,`ts`),
  KEY `ADDTIME` (`add_time`),
  KEY `UPDATETIME` (`update_time`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
'''

create_xiabb_category_table_sql = '''
CREATE TABLE IF NOT EXISTS `xiabb_category` (
    `id` int(11) NOT NULL AUTO_INCREMENT,
    `name` varchar(255) NOT NULL,
    `parent` varchar(255) DEFAULT NULL,
    `add_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
    `update_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (`id`),
    UNIQUE KEY `UK_RAW` (`name`,`parent`),
    KEY `ADDTIME` (`add_time`),
    KEY `UPDATETIME` (`update_time`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
'''

def store_crawler_data(path):
    logging.info("start to store data in "+path)
    db = None
    cnt = 0
    try:
        db = MySQLdb.connect(host=mysql_conf['host'], user=mysql_conf['user'],
                             passwd=mysql_conf['password'], db=mysql_conf['database'], charset='utf8')
        cursor = db.cursor()
        cursor.execute(create_xiabb_category_table_sql)
        cursor.execute(create_xiabb_product_table_sql)
        cursor.execute(create_xiabb_product_price_table_sql)
        cursor.execute(create_xiabb_product_img_table_sql)
        db.commit()
        with open(path) as f:
            for line in f:
                cnt += 1
                try:
                    p = json.loads(line.strip())
                    if 'url' in p:
                        h = hashlib.md5(p['url']).hexdigest()
                        url = 'crawler.xiabb.me/' + h
                        sql = """
                            INSERT INTO %s (name,en_name,category,brand,info,url,remote_url)
                            VALUES ("%s","%s","%s","%s","%s","%s","%s")
                            ON DUPLICATE KEY
                            UPDATE en_name = "%s", url="%s", remote_url="%s"
                            """ % (
                            table_product,
                            MySQLdb.escape_string(p['name'].encode('utf-8')),
                            MySQLdb.escape_string(p['en_name'].encode('utf-8')),
                            MySQLdb.escape_string(p['category'].encode('utf-8')),
                            MySQLdb.escape_string(p['brand'].encode('utf-8')),
                            MySQLdb.escape_string("\t".join(p['info']).encode('utf-8')),
                            MySQLdb.escape_string(url.encode('utf-8')),
                            MySQLdb.escape_string(p['url'].encode('utf-8')),
                            MySQLdb.escape_string(p['en_name'].encode('utf-8')),
                            MySQLdb.escape_string(url.encode('utf-8')),
                            MySQLdb.escape_string(p['url'].encode('utf-8')),
                        )
                        cursor.execute(sql)
                        sql = """
                            INSERT INTO %s (product, brand, price, ts)
                            VALUES("%s", "%s", %s, "%s")
                            ON DUPLICATE KEY
                            UPDATE price = %s
                        """ % (
                            table_product_price,
                            MySQLdb.escape_string(p['name'].encode('utf-8')),
                            MySQLdb.escape_string(p['brand'].encode('utf-8')),
                            MySQLdb.escape_string(p['price'].encode('utf-8')),
                            MySQLdb.escape_string(p['ts'].encode('utf-8')),
                            MySQLdb.escape_string(p['price'].encode('utf-8')),
                        )
                        cursor.execute(sql)

                        for img_url in p['img_lg_url']+p['img_sm_url']:
                            sql = """
                                INSERT INTO %s (product, brand, type, url) VALUES("%s", "%s", 1, "%s")
                                ON DUPLICATE KEY
                                UPDATE product = "%s", brand = "%s"
                            """ %(
                                table_product_img,
                                MySQLdb.escape_string(p['name'].encode('utf-8')),
                                MySQLdb.escape_string(p['brand'].encode('utf-8')),
                                MySQLdb.escape_string(img_url.encode('utf-8')),
                                MySQLdb.escape_string(p['name'].encode('utf-8')),
                                MySQLdb.escape_string(p['brand'].encode('utf-8')),
                            )
                            cursor.execute(sql)

                        sql = """
                            INSERT INTO %s (brand, type, url) VALUES("%s", 2, "%s")
                            ON DUPLICATE KEY
                            UPDATE brand = "%s"
                        """ % (
                            table_product_img,
                            MySQLdb.escape_string(p['brand'].encode('utf-8')),
                            MySQLdb.escape_string(img_url.encode('utf-8')),
                            MySQLdb.escape_string(p['brand'].encode('utf-8')),
                        )
                        cursor.execute(sql)

                        if cnt % max_cnt == 0:
                            db.commit()
                    else:
                        if p['parent'] is not None:
                            sql = '''
                                INSERT INTO %s (name, parent) VALUES("%s", "%s")
                            ''' % (table_category,
                                   MySQLdb.escape_string(p['name'].encode('utf-8')),
                                   MySQLdb.escape_string(p['parent'].encode('utf-8')))
                        else:
                            sql = '''
                                INSERT INTO %s (name) VALUES("%s")
                            ''' % (table_category,
                                   MySQLdb.escape_string(p['name'].encode('utf-8')),
                                )
                        cursor.execute(sql)
                        if cnt % max_cnt == 0:
                            db.commit()
                except Exception, e:
                    print >> sys.stderr, e
    except Exception, e:
        print >> sys.stderr, e
    finally:
        logging.info("done ("+str(cnt) + ")")
        if db:
            db.commit()
            db.close()


def fetch_product_data():
    rows = []
    res = []
    db = None
    try:
        db = MySQLdb.connect(host=mysql_conf['host'], user=mysql_conf['user'],
                             passwd=mysql_conf['password'], db=mysql_conf['database'], charset='utf8')
        cursor = db.cursor()
        sql = """
            SELECT name,en_name,url,category,brand,info,remote_url
            FROM %s
            """ % table_product
        cursor.execute(sql)
        rows = cursor.fetchall()
    except Exception, e:
        print >> sys.stderr, e
    finally:
        if db:
            db.close()
    for row in rows:
        res.append({
            'name': row[0],
            'en_name': row[1],
            'url': row[2],
            'category': row[3],
            'brand': row[4],
            'info': row[5],
            'remote_url': row[6]
        })
    return res

if __name__ == "__main__":
    filepath = '/data/crawlers/sephora_cn/sephora_cn.jl'
    if len(sys.argv) > 1:
        filepath = sys.argv[1]
    store_crawler_data(filepath)