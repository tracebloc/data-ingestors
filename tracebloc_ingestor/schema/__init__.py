# Marker file so setuptools.find_packages() treats this directory as a
# subpackage of tracebloc_ingestor. Without this, the package_data
# declaration ``"tracebloc_ingestor.schema": ["*.json"]`` in setup.py
# is a silent no-op — setuptools refuses to attach data files to a
# directory it doesn't know is a package, and the bundled
# ``ingest.v1.json`` ends up missing from the built wheel.
# Discovered during real-cluster validation (2026-05-19): the ingestor
# Job crashed with FileNotFoundError on the schema path inside the
# installed site-packages tree.
