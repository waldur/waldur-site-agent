#!/usr/bin/env python

from setuptools import setup

install_requires = [
    "python-waldur-client>=0.2.9",
    "requests==2.27.1",
    "PyYAML==6.0.1",

]

tests_requires = [
    "freezegun==0.3.4",
    "pytest==7.1.2",
]

setup(
    name="waldur-slurm-agent",
    version="0.1.0",
    author="OpenNode Team",
    author_email="info@opennodecloud.com",
    url="https://docs.waldur.com",
    license="MIT",
    description="SLURM integration module for Waldur.",
    long_description=open("README.md").read(),
    long_description_content_type="text/markdown",
    install_requires=install_requires,
    tests_require=tests_requires,
    packages=['waldur_slurm', 'waldur_slurm.slurm_client'],
    classifiers=[
        "Intended Audience :: Developers",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
)
