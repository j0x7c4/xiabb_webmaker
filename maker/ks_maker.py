# encoding=utf-8
from jinja2 import Environment, PackageLoader
from utils import ks_manager as km
import sys

def make_pages (exp_dir):
    env = Environment(loader=PackageLoader('page', 'templates'))
    template = env.get_template('ks_project/base.html')
    for page in km.fetch_crawler_data():
        url = page['url']
        exp_path = exp_dir + '/' + url.split('/')[1] + ".html"
        with open(exp_path, 'w') as f:
            f.write(template.render(page).encode('utf-8'))

if __name__ == "__main__":
    if len(sys.argv)>1:
        make_pages(sys.argv[1])
