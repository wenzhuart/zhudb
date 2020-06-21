# -*- coding: utf-8 -*-

from setuptools import setup, find_packages

setup(
    name='zhudb',
    version='0.1.8',
    author='bamboo',
    author_email='wenzhu_art@hotmail.com',
    url='https://github.com/wenzhuart/zhudb',
    description=u'A sql tool in python3',
    packages=find_packages(),
    install_requires=['pymysql']
)
