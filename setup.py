import sys
from setuptools import setup

extra = {}
if sys.version_info < (2, 7):
    extra['install_requires'] = ['argparse']

setup(
    name='scribe-monitor',
    version='0.1',
    author='Jure Ham',
    license='Apache 2.0',
    author_email='jure.ham@zemanta.com',
    description="Monitor scribe server to statsd",
    url='https://github.com/Zemanta/scribemonitor',
    packages=['scribemonitor'],
    install_requires=['statsd', 'scribe'] + extra.get('install_requires', []),
    platforms='any',
    scripts=['scribemonitor/scribe_monitor'],
    zip_safe=False,
)
