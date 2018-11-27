from setuptools import setup, find_packages

setup(
    name='opencastcsvschedule',
    author='UIS DevOps',
    packages=find_packages(),
    install_requires=[
        'docopt',
        'python-dateutil',
        'pytz',
        'requests',
        'requests-toolbelt',
    ],
    entry_points={
        'console_scripts': [
            'opencast_csv_schedule=opencastcsvschedule:main'
        ]
    },
)
