from transforms.api import Pipeline

from trinetx import datasets, anchor


my_pipeline = Pipeline()
my_pipeline.discover_transforms(datasets, anchor)
