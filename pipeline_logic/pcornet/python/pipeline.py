from transforms.api import Pipeline

from pcornet import datasets


my_pipeline = Pipeline()
my_pipeline.discover_transforms(datasets)
