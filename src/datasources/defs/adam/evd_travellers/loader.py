"""
ADaM Travellers Loader
"""

from . import config
from ..common import create_source

source = create_source(config)

pipeline = dlt.pipeline(
    pipeline_name=f"adam_{RESOURCE_NAME}",
    destination="filesystem",
    dataset_name="adam_raw",
)