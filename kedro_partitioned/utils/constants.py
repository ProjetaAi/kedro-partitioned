"""Package for storing constants."""
import os


MAX_WORKERS = int(os.environ.get('MAX_WORKERS', os.cpu_count() or 1))
"""Maximum number of cpus to use in parallel."""
MAX_NODES = int(os.environ.get('MAX_NODES', 1))
"""Maximum number of cluster computers."""
