import os
from setuptools import setup, find_packages

version = '0.0.1'

install_requires = ['asyncio']

def read(f):
    return open(os.path.join(os.path.dirname(__file__), f)).read().strip()

setup(name='aiogear',
      version=version,
      description=('gearman client/worker for asyncio'),
      long_description=read('README.md'),
      classifiers=[
          'License :: OSI Approved :: BSD License',
          'Intended Audience :: Developers',
          'Programming Language :: Python',
          'Programming Language :: Python :: 3.3'],
      author='Li Meng Jun',
      author_email='lmjubuntu@gmail.com',
      # url='https://github.com/fafhrd91/asynchttp/',
      license='BSD',
      packages=find_packages(),
      install_requires = install_requires,
      include_package_data = True)
