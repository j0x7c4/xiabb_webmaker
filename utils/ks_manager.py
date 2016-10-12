# encoding=utf-8
import MySQLdb
import json
import sys
from settings import mysql_conf, log_conf
import hashlib
import logging
logging.basicConfig(filename=log_conf["filename"],format=log_conf["format"],level=log_conf["level"])
table = 'ks_project'
max_cnt = 100

create_table_sql = '''
CREATE TABLE IF NOT EXISTS `ks_project` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `name` varchar(255) NOT NULL,
  `creator` varchar(255) NOT NULL,
  `url` varchar(255) NOT NULL,
  `category_tag` varchar(255) DEFAULT NULL,
  `content` text NOT NULL,
  `backers_count` int(11) DEFAULT NULL,
  `pledged` varchar(255) DEFAULT NULL,
  `location` varchar(255) DEFAULT NULL,
  `remote_url` varchar(255) NOT NULL,
  `add_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `update_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`,`update_time`),
  UNIQUE KEY `UK_NAME_CREATOR` (`name`,`creator`),
  KEY `ADDTIME` (`add_time`),
  KEY `UPDATETIME` (`update_time`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
'''


def store_crawler_data(path):
    logging.info("start to store data in "+path)
    db = None
    try:
        db = MySQLdb.connect(host=mysql_conf['host'], user=mysql_conf['user'],
                             passwd=mysql_conf['password'], db=mysql_conf['database'], charset='utf8')
        cursor = db.cursor()
        cursor.execute(create_table_sql)
        db.commit()
        with open(path) as f:
            cnt = 0
            for line in f:
                cnt += 1
                try:
                    p = json.loads(line.strip())
                    p['backers_count'] = p['backers_count'].replace(',', '')
                    h = hashlib.md5(p['url']).hexdigest()
                    url = 'crawler.xiabb.me/' + h
                    sql = """
                    INSERT INTO %s (name,creator,url,category_tag,content,backers_count,pledged,location,remote_url)
                    VALUES ("%s","%s","%s","%s","%s",%s,"%s","%s","%s")
                    ON DUPLICATE KEY
                    UPDATE url="%s", category_tag="%s", content="%s", backers_count=%s, pledged="%s", location="%s", remote_url="%s"
                    """ % (
                        table,
                        MySQLdb.escape_string(p['name'].encode('utf-8')),
                        MySQLdb.escape_string(p['creator'].encode('utf-8')),
                        MySQLdb.escape_string(url.encode('utf-8')),
                        MySQLdb.escape_string(p['category_tag'].encode('utf-8')),
                        MySQLdb.escape_string(p['content'].encode('utf-8')),
                        MySQLdb.escape_string(p['backers_count'].encode('utf-8')),
                        MySQLdb.escape_string(p['pledged'].encode('utf-8')),
                        MySQLdb.escape_string(p['location'].encode('utf-8')),
                        MySQLdb.escape_string(p['url'].encode('utf-8')),
                        MySQLdb.escape_string(url.encode('utf-8')),
                        MySQLdb.escape_string(p['category_tag'].encode('utf-8')),
                        MySQLdb.escape_string(p['content'].encode('utf-8')),
                        MySQLdb.escape_string(p['backers_count'].encode('utf-8')),
                        MySQLdb.escape_string(p['pledged'].encode('utf-8')),
                        MySQLdb.escape_string(p['location'].encode('utf-8')),
                        MySQLdb.escape_string(p['url'].encode('utf-8')),
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


def fetch_crawler_data():
    rows = []
    res = []
    db = None
    try:
        db = MySQLdb.connect(host=mysql_conf['host'], user=mysql_conf['user'],
                             passwd=mysql_conf['password'], db=mysql_conf['database'], charset='utf8')
        cursor = db.cursor()
        sql = """
            SELECT name, creator, url, category_tag, content, backers_count, pledged, location, remote_url
            FROM %s
            """ % table
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
            'creator': row[1],
            'url': row[2],
            'category_tag': row[3],
            'content': row[4],
            'backers_count': row[5],
            'pledged': row[6],
            'location': row[7],
            'remote_url': row[8],
        })
    return res

if __name__ == "__main__":
    filepath = '/data/crawlers/ks/ks.jl'
    if len(sys.argv)>1:
        filepath = sys.argv[1]
    store_crawler_data(filepath)
