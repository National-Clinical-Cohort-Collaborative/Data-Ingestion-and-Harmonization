from transforms.api import Pipeline

from act import datasets


my_pipeline = Pipeline()
my_pipeline.discover_transforms(datasets)
