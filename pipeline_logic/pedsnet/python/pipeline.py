from transforms.api import Pipeline

from myproject import datasets


my_pipeline = Pipeline()
my_pipeline.discover_transforms(datasets)
