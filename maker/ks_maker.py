# encoding=utf-8
from jinja2 import Environment, PackageLoader
from utils import ks_manager as km

if __name__ == "__main__":
    env = Environment(loader=PackageLoader('post', 'templates'))
    template = env.get_template('ks_project/base.html')
    for page in km.fetch_crawler_data():
        print template.render(page)