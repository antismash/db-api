import os
from setuptools import setup

version_py = os.path.join('api', 'version.py')
with open(version_py, 'r') as fh:
    version = fh.read().strip().split('=')[-1].replace("'", "")

with open('requirements.txt', 'r') as fh:
    install_requires = []
    for line in fh.readlines():
        line = line.strip()
        if '://' in line:
            line = line.split('=')[-1]
        install_requires.append(line)

tests_require_txt = os.path.join('tests', 'requirements.txt')
with open(tests_require_txt, 'r') as fh:
    tests_require = [l.strip() for l in fh.readlines()]

setup(name='antismash.db.api',
      version=version,
      install_requires=install_requires,
      tests_require=tests_require,
      author='Kai Blin',
      author_email='kblin@biosustain.dtu.dk',
      description='A REST-like web API for antismash DB',
      packages=['api'],
      zip_safe=False)
