#!/usr/bin/env python

from setuptools import setup

install_requires = [
    "python-waldur-client@git+https://github.com/waldur/python-waldur-client",
    "requests==2.27.1",
    "PyYAML==6.0",

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
    package_dir={"": "waldur_slurm"},
    classifiers=[
        "Intended Audience :: Developers",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
)
