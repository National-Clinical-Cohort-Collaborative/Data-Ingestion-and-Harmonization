from transforms.api import Pipeline

from trinetx import datasets


my_pipeline = Pipeline()
my_pipeline.discover_transforms(datasets)
