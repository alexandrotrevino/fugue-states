from setuptools import setup, find_packages

setup(
    name='fuguestates',
    version=0.1,
    packages=find_packages(),
    install_requires=[
        'metawear>=1.0.8',
        'python-osc>=1.8.3'
    ]
)