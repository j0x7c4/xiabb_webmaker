# encoding=utf-8
from jinja2 import Environment, PackageLoader
env = Environment(loader=PackageLoader('post', 'templates'))
template = env.get_template('base.html')
print template.render(title="abc", content="cba")