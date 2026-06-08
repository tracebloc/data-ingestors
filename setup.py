from setuptools import setup, find_packages
import os
import re

# read the contents of your README file
from pathlib import Path

this_directory = Path(__file__).parent
long_description = (this_directory / "Readme.md").read_text()


def _read_version():
    """Single-source the version from tracebloc_ingestor/__init__.py.

    Parsed as text (not imported) so building the sdist never has to import the
    package or its dependencies. Keeping the version literal in exactly one
    place is what stops setup.py and __version__ drifting again (#175).
    """
    init_py = (this_directory / "tracebloc_ingestor" / "__init__.py").read_text()
    match = re.search(r'^__version__\s*=\s*["\']([^"\']+)["\']', init_py, re.M)
    if match is None:
        raise RuntimeError(
            "Unable to find __version__ in tracebloc_ingestor/__init__.py"
        )
    return match.group(1)


with open("requirements.txt", "r") as f:
    requirements = f.read().splitlines()

setup(
    name="tracebloc_ingestor",
    version=_read_version(),
    author="Tracebloc",
    author_email="support@tracebloc.com",
    description="A flexible data ingestion library for various file formats",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/tracebloc/data-ingestors",
    packages=find_packages(),
    package_data={
        "": ["Readme.md"],
        "tracebloc_ingestor.schema": ["*.json"],
    },
    include_package_data=True,
    entry_points={
        # Registered as the official image's entrypoint (Ticket #45). The Helm
        # subchart (Ticket client#86) sets INGEST_CONFIG to the path of the
        # mounted ingest.yaml; this console script reads it, validates against
        # schema/ingest.v1.json, and dispatches to the right ingestor.
        "console_scripts": [
            "tracebloc-ingest = tracebloc_ingestor.cli.run:main",
        ],
    },
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: Apache Software License",
        "Operating System :: OS Independent",
    ],
    python_requires=">=3.11",
    install_requires=requirements,
)
