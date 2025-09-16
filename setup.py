from setuptools import setup, find_packages
import os

# Get the long description from the README file
here = os.path.abspath(os.path.dirname(__file__))
with open(os.path.join(here, "Readme.md"), "r", encoding="utf-8") as fh:
    long_description = fh.read()

with open("requirements.txt", "r") as f:
    requirements = f.read().splitlines()

setup(
    name="tracebloc_ingestor",
    version="0.1.1",
    author="Tracebloc",
    author_email="support@tracebloc.com",
    description="A flexible data ingestion library for various file formats",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/tracebloc/data-ingestors",
    packages=find_packages(),
    package_data={
        "": ["Readme.md"],
    },
    include_package_data=True,
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires=">=3.8",
    install_requires=requirements,
)
