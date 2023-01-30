"""
This is a very precisely created file, do not change it. It was created to trick Foundry Templates into giving us the
path of the root folder of the deployed template. In generate-anchor.py, we use the anchor path defined in path.py to
create a dummy anchor dataset at the root of the project. Then when a new instance of the template is deployed, this
anchor path is automatically replaced with the path of the anchor dataset in the deployed template. Then to get the
root, we simply remove the name "anchor". Finally, we can use this root path in the rest of the repo. Doing this
allowed us to massively de-duplicate repeated code, in some steps reducing the number of lines of code by more than 90%.
"""

from transforms.api import transform_df, Output
from act.anchor import path


@transform_df(
    Output(path.anchor)
)
def compute(ctx):
    return ctx.spark_session.range(1)
