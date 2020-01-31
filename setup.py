from setuptools import setup, find_packages

requires = [
    'xlsxwriter',
    'sqlalchemy',
    'zope.sqlalchemy',
]

setup(
    name='sparkperfreport',
    version='0.1.0',
    description='Get report for SparkPerf metrics in XLS format',
    author='McWladkoE',
    author_email='svevladislav@gmail.com',
    url='',
    packages=find_packages('src'),
    package_dir={'': 'src'},
    install_requires=requires,
    entry_points="""\
        [console_scripts]
        sparkperfreport_report = sparkperfreport.report:main
        sparkperfreport_initialize_db = sparkperfreport.initialize_db:main
    """,
)
