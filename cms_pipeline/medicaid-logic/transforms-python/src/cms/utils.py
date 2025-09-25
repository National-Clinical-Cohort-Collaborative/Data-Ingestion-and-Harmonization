import pyspark.sql.functions as F


def flat(list_of_lists):
    for sublist in list_of_lists:
        for element in sublist:
            yield element


def melt_array(
    df,
    pkey_cols,
    cols_to_melt,
    var_name,
    value_name,
    num_start_idx,
    remove_nulls=True
):
    """
    Many of the variables in CMS datasets are arrays that have been exploded
    into many columns - e.g. DGNSCD01, DGNSCD02, ..., DGNSCDXX

    This function 'melts' the array into a dataset of the form:

    PRIMARY KEY COLUMNS, DGNSCD_col, DGCNSCD, col_num
                          DGNSCD01 ,  XXXX. ,   01


    The col_num is useful if you want to join two such arrays (e.g. when
    combining diagnosis/procedure information with dates)
    """
    df = df.select(pkey_cols + cols_to_melt)

    df_long = melt_df(
        df,
        id_vars=pkey_cols,
        value_vars=cols_to_melt,
        var_name=var_name,
        value_name=value_name
    )
    if remove_nulls:
        df_long = df_long.filter(
            (F.col(value_name).isNotNull()) & (F.col(value_name) != "")
        )
    df_long = df_long.withColumn("col_num", F.substring(var_name, num_start_idx, 2))
    return df_long


def melt_df(df, id_vars, value_vars, var_name, value_name):
    """Convert :class:`DataFrame` from wide to long format."""

    # Create array<struct<variable: str, value: ...>>
    _vars_and_vals = F.array(*(
        F.struct(F.lit(c).alias(var_name), F.col(c).alias(value_name))
        for c in value_vars))

    # Add to the DataFrame and explode
    _tmp = df.withColumn("_vars_and_vals", F.explode(_vars_and_vals))

    cols = id_vars + [
            F.col("_vars_and_vals")[x].alias(x) for x in [var_name, value_name]]
    return _tmp.select(*cols)


def fixedSizeHash(column, pepper, n_bits):
    return F.conv(
                  F.substring(
                        F.md5(F.concat(column, F.lit(pepper))),
                        1, 15),
                    16, 10
                  ).cast("long") \
                  .bitwiseAND(F.lit((1 << n_bits) - 1))  # bitwise AND gives you the first n_bits bits


FINAL_OPT_IN_COLS = [
    'data_partner_id',
    'cms_person_id',
    'n3c_person_id'
]
