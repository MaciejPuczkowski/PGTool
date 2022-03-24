from distutils.core import setup
setup(
  name='asyncpgtool',
  packages=['apgtool'],
  version='0.0.4',
  license='MIT',
  description='tool enhancing work with asyncpg',
  author='Maciej Puczkowski',
  author_email='puczkowski@gmail.com',
  keywords=['postgres', 'pg', 'asyncpg', 'sql', 'db', 'database', 'orm'],
  install_requires=['asyncpg~=0.25.0', 'aiofiles~=0.8.0', 'pydantic~=1.9.0'],
  classifiers=[
    'Development Status :: 3 - Alpha',
    'Intended Audience :: Developers',
    'Topic :: Software Development :: Build Tools',
    'License :: OSI Approved :: MIT License',
    'Programming Language :: Python :: 3',
    'Programming Language :: Python :: 3.9',
    'Programming Language :: Python :: 3.10',
  ],
)