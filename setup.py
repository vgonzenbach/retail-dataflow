# setup.py
from setuptools import setup, find_packages

setup(
    name="gymshark_events",
    version="0.0.1",
    packages=find_packages(),
    install_requires=[
        "apache-beam[gcp]",
    ],
)
