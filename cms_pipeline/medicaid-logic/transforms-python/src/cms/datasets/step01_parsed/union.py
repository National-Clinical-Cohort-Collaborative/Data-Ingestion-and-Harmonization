from pyspark.sql import functions as F
from transforms.api import transform, InputSet, Output
from functools import reduce
from cms.configs import parsed_path, union_path
from cms.configs import taf_de_base, taf_de_dates, taf_de_dsb, taf_de_hh_and_spo, taf_de_mc, taf_de_mfp, taf_de_wvr, taf_ip, taf_lt, taf_ot, taf_rx


@transform(
    taf_de_base_union=Output(union_path + "de_base"),
    taf_de_base=InputSet([parsed_path+taf_de_base[0], parsed_path+taf_de_base[1], parsed_path+taf_de_base[2], parsed_path+taf_de_base[3]]),

    taf_de_dates_union=Output(union_path + "de_dates"),
    taf_de_dates=InputSet([parsed_path+taf_de_dates[0], parsed_path+taf_de_dates[1], parsed_path+taf_de_dates[2], parsed_path+taf_de_dates[3], parsed_path+taf_de_dates[4]]),

    taf_de_dsb_union=Output(union_path + "de_dsb"),
    taf_de_dsb=InputSet([parsed_path+taf_de_dsb[0], parsed_path+taf_de_dsb[1], parsed_path+taf_de_dsb[2], parsed_path+taf_de_dsb[3], parsed_path+taf_de_dsb[4]]),

    taf_de_hh_and_spo_union=Output(union_path + "de_hh_and_spo"),
    taf_de_hh_and_spo=InputSet([parsed_path+taf_de_hh_and_spo[0], parsed_path+taf_de_hh_and_spo[1], parsed_path+taf_de_hh_and_spo[2], parsed_path+taf_de_hh_and_spo[3], parsed_path+taf_de_hh_and_spo[4]]),

    taf_de_mc_union=Output(union_path + "de_mc"),
    taf_de_mc=InputSet([parsed_path+taf_de_mc[0], parsed_path+taf_de_mc[1], parsed_path+taf_de_mc[2], parsed_path+taf_de_mc[3], parsed_path+taf_de_mc[4]]),

    taf_de_mfp_union=Output(union_path + "de_mfp"),
    taf_de_mfp=InputSet([parsed_path+taf_de_mfp[0], parsed_path+taf_de_mfp[1], parsed_path+taf_de_mfp[2], parsed_path+taf_de_mfp[3], parsed_path+taf_de_mfp[4]]),

    taf_de_wvr_union=Output(union_path + "de_wvr"),
    taf_de_wvr=InputSet([parsed_path+taf_de_wvr[0], parsed_path+taf_de_wvr[1], parsed_path+taf_de_wvr[2], parsed_path+taf_de_wvr[3], parsed_path+taf_de_wvr[4]]),

    taf_ip_union=Output(union_path + "ip"),
    taf_ip=InputSet([parsed_path+taf_ip[0], parsed_path+taf_ip[1], parsed_path+taf_ip[2], parsed_path+taf_ip[3], parsed_path+taf_ip[4]]),

    taf_lt_union=Output(union_path + "lt"),
    taf_lt=InputSet([parsed_path+taf_lt[0], parsed_path+taf_lt[1], parsed_path+taf_lt[2], parsed_path+taf_lt[3], parsed_path+taf_lt[4]]),

    taf_ot_union=Output(union_path + "ot"),
    taf_ot=InputSet([parsed_path+taf_ot[0], parsed_path+taf_ot[1], parsed_path+taf_ot[2], parsed_path+taf_ot[3]]),  # , parsed_path+taf_ot[4]]),

    taf_rx_union=Output(union_path + "rx"),
    taf_rx=InputSet([parsed_path+taf_rx[0], parsed_path+taf_rx[1], parsed_path+taf_rx[2], parsed_path+taf_rx[3], parsed_path+taf_rx[4]]),
)
def compute_function(taf_de_base_union,
                     taf_de_base,
                     taf_de_dates_union,
                     taf_de_dates,
                     taf_de_dsb_union,
                     taf_de_dsb,
                     taf_de_hh_and_spo_union,
                     taf_de_hh_and_spo,
                     taf_de_mc_union,
                     taf_de_mc,
                     taf_de_mfp_union,
                     taf_de_mfp,
                     taf_de_wvr_union,
                     taf_de_wvr,
                     taf_ip_union,
                     taf_ip,
                     taf_lt_union,
                     taf_lt,
                     taf_ot_union,
                     taf_ot,
                     taf_rx_union,
                     taf_rx):
    dataframes = [df.dataframe() for df in taf_de_base]
    unioned_dfs = reduce(F.DataFrame.unionAll, dataframes)
    taf_de_base_union.write_dataframe(unioned_dfs)

    dataframes = [df.dataframe() for df in taf_de_dates]
    unioned_dfs = reduce(F.DataFrame.unionAll, dataframes)
    taf_de_dates_union.write_dataframe(unioned_dfs)

    dataframes = [df.dataframe() for df in taf_de_dsb]
    unioned_dfs = reduce(F.DataFrame.unionAll, dataframes)
    taf_de_dsb_union.write_dataframe(unioned_dfs)

    dataframes = [df.dataframe() for df in taf_de_hh_and_spo]
    unioned_dfs = reduce(F.DataFrame.unionAll, dataframes)
    taf_de_hh_and_spo_union.write_dataframe(unioned_dfs)

    dataframes = [df.dataframe() for df in taf_de_mc]
    unioned_dfs = reduce(F.DataFrame.unionAll, dataframes)
    taf_de_mc_union.write_dataframe(unioned_dfs)

    dataframes = [df.dataframe() for df in taf_de_mfp]
    unioned_dfs = reduce(F.DataFrame.unionAll, dataframes)
    taf_de_mfp_union.write_dataframe(unioned_dfs)

    dataframes = [df.dataframe() for df in taf_de_wvr]
    unioned_dfs = reduce(F.DataFrame.unionAll, dataframes)
    taf_de_wvr_union.write_dataframe(unioned_dfs)

    dataframes = [df.dataframe() for df in taf_ip]
    unioned_dfs = reduce(F.DataFrame.unionAll, dataframes)
    taf_ip_union.write_dataframe(unioned_dfs)

    dataframes = [df.dataframe() for df in taf_lt]
    unioned_dfs = reduce(F.DataFrame.unionAll, dataframes)
    taf_lt_union.write_dataframe(unioned_dfs)

    dataframes = [df.dataframe() for df in taf_ot]
    unioned_dfs = reduce(F.DataFrame.unionAll, dataframes)
    taf_ot_union.write_dataframe(unioned_dfs)

    dataframes = [df.dataframe() for df in taf_rx]
    unioned_dfs = reduce(F.DataFrame.unionAll, dataframes)
    taf_rx_union.write_dataframe(unioned_dfs)
