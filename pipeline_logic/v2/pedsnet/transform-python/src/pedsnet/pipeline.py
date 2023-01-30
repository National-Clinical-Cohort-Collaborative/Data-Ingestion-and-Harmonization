from transforms.api import Pipeline
from pedsnet import datasets, anchor


my_pipeline = Pipeline()
my_pipeline.discover_transforms(datasets, anchor)
