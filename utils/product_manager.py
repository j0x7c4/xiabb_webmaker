# encoding=utf-8
import MySQLdb
import json
import sys
from settings import mysql_conf, log_conf
import hashlib
import logging
logging.basicConfig(filename=log_conf["filename"],format=log_conf["format"],level=log_conf["level"])

max_cnt = 100

table_raw_product = 'xiabb_raw_product'
table_category = 'xiabb_category'

create_xiabb_raw_product_table_sql = '''
CREATE TABLE IF NOT EXISTS `xiabb_raw_product` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `name` varchar(255) NOT NULL,
  `en_name` varchar(255) NOT NULL,
  `brand` varchar(255) NOT NULL,
  `url` varchar(255) NOT NULL,
  `price` float DEFAULT NULL,
  `info` varchar(255) DEFAULT NULL,
  `category` varchar(255) DEFAULT NULL,
  `img_url` text DEFAULT NULL,
  `brand_img_url` text DEFAULT NULL,
  `remote_url` varchar(255) NOT NULL,
  `add_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `update_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  `ts` datetime NOT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `UK_RAW` (`name`,`brand`,`category`, `ts`),
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
    `ts` datetime NOT NULL,
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
        cursor.execute(create_xiabb_raw_product_table_sql)
        cursor.execute(create_xiabb_category_table_sql)
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
                           INSERT INTO %s (name,en_name,url,category,brand,price,info,ts,remote_url,img_url, brand_img_url)
                           VALUES ("%s","%s","%s","%s","%s",%s,"%s","%s","%s","%s","%s")
                           ON DUPLICATE KEY
                           UPDATE url="%s", remote_url="%s"
                           """ % (
                            table_raw_product,
                            MySQLdb.escape_string(p['name'].encode('utf-8')),
                            MySQLdb.escape_string(p['en_name'].encode('utf-8')),
                            MySQLdb.escape_string(url.encode('utf-8')),
                            MySQLdb.escape_string(p['category'].encode('utf-8')),
                            MySQLdb.escape_string(p['brand'].encode('utf-8')),
                            MySQLdb.escape_string(p['price'].encode('utf-8')),
                            MySQLdb.escape_string("\t".join(p['info']).encode('utf-8')),
                            MySQLdb.escape_string(p['ts'].encode('utf-8')),
                            MySQLdb.escape_string(p['url'].encode('utf-8')),
                            MySQLdb.escape_string(",".join(p['img_lg_url']+p['img_sm_url'])),
                            MySQLdb.escape_string(p['brand_img_url']),
                            MySQLdb.escape_string(url.encode('utf-8')),
                            MySQLdb.escape_string(p['url'].encode('utf-8')),
                        )
                    else:
                        if p['parent'] is not None:
                            sql = '''
                                INSERT INTO %s (name, parent, ts) VALUES("%s", "%s", "%s")
                                ON DUPLICATE KEY
                                UPDATE ts = "%s"
                            ''' % (table_category,
                                   MySQLdb.escape_string(p['name'].encode('utf-8')),
                                   MySQLdb.escape_string(p['parent'].encode('utf-8')),
                                   MySQLdb.escape_string(p['ts'].encode('utf-8')),
                                   MySQLdb.escape_string(p['ts'].encode('utf-8')))
                        else:
                            sql = '''
                                INSERT INTO %s (name, ts) VALUES("%s", "%s")
                                ON DUPLICATE KEY
                                UPDATE ts = "%s"
                            ''' % (table_category,
                                   MySQLdb.escape_string(p['name'].encode('utf-8')),
                                   MySQLdb.escape_string(p['ts'].encode('utf-8')),
                                   MySQLdb.escape_string(p['ts'].encode('utf-8')))

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
            SELECT name,en_name,url,category,brand,price,info,ts,remote_url,img_url, brand_img_url
            FROM %s
            """ % table_raw_product
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
            'price': row[5],
            'info': row[6],
            'ts': row[7],
            'remote_url': row[8],
            'img_url': row[9].split(','),
            'brand_img_url': row[10]
        })
    return res

if __name__ == "__main__":
    filepath = '/data/crawlers/sephora_cn/sephora_cn.jl'
    if len(sys.argv) > 1:
        filepath = sys.argv[1]
    store_crawler_data(filepath)
