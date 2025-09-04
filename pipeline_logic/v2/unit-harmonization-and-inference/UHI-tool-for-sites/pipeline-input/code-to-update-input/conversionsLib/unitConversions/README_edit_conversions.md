# Editing conversions
# N3C Unit Harmonization README
This README provides instructions on how to add, remove or edit measurement unit conversions used for the harmonization and inference of units in N3C.

A "conversion" indicates the following four items:
    1. non-canonical units to convert from
    2. canonical unit of measure to convert to
    3. the specified measurement concept for the conversion
    3. the conversion formula to use

(Note that the same unit conversion may require different formulae for similar measurement concepts within labs e.g. D-dimer FEU and DDU, and across different labs)

Steps for deriving and writing conversions and conversion formulas:
    1. Use canonical_units_of_measure to find the codesets and target (canonical) units for each lab involved in units harmonization and inference.
    https://unite.nih.gov/workspace/data-integration/dataset/preview/ri.foundry.main.dataset.09b4a60a-3da4-4754-8a7e-0b874e2a6f2b/master
    2. check which conversions we already have for your lab(s) of interest in this repository (conversionsDictionary.py) to not duplicate conversions
    3. Perform analyses to determine the required conversions such that you derive conversion in the form
      "<non-canonical unit_concept_name>_to_<canonical unit_concept_name>_for_<measurement_concept_name>": lambda x_col: conversion,
      example: "billion per liter_to_thousand per microliter_for_Leukocytes [#/volume] in Blood by Automated count": lambda x_col: x_col,
    Note:
        - even if the value is not to change, we need a formula (lambda x_col: x_col)
        - canonical unit to canonical unit for measurement concepts is not required for conversions, this is accounted for in the pipeline
        - any conversion involving 'null', 'No matching concept', 'No information', or 'Other' as the unit_concept_name is already accounted for in out unit inference
        pipeline, so please do not add conversions involving these unit_concept_name values. Only include conversions with valid unit_concept_name that have a conversion
        to the canonical unit that is not already in the conversionsDictionary.py dictionary of conversions.
        - if you see data in the patient tables that has a valid conversion already, yet isn't harmonized, it will likely be because the values fall outside of accepted value ranges
        stated in canonical_units_of_measure here: https://unite.nih.gov/workspace/data-integration/dataset/preview/ri.foundry.main.dataset.09b4a60a-3da4-4754-8a7e-0b874e2a6f2b/master


    To help others, please optionally deposit the code you used to assess and derive required conversions to the Knowledge Store
    with '[UH&I]' (Unit Harmonization and Inferent) at the start of your Knowledge Object name.


Steps for editing the conversions in this repository (input conversions to the N3C units harmonization and inference process):
    1. within this repository make a branch with a new tag number (following on from previous pattern of tag numbers indicated from branches)
    2. add your conversions to the python dictionary on that branch
    3. create a PR (pull request) to merge your branch to master, linking all relevant analyses that justify the conversions, where possible
    4. wait to receive approval from each of the following groups:

          units_approvers_clinicians
          units_approvers_informaticians
          Palantir staff

    5. merge branch to master
    6. Create a tag:
        Navigate to the Branches tab
          --> Tags
              --> + New Tag button
                  --> Create a tag at master branch
                      --> Name the tag exactly the same as the branch you just merged
                          --> click Create tag button
        General information on tags can be found here under the Publish the Function section https://unite.nih.gov/workspace/documentation/product/functions/getting-started
    7. email kbradwell@palantir.com to let us know the new tag is available for use in our LDS harmonization pipeline, with the tag number in the subject line

    The team at Palantir or DI&H will then update the tag for the conversions dictionary in the LDS harmonization repository so that the new published dictionary
    is taken as input.
