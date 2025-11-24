import os
import json
from utils import QueryFromJson
from snowflake.snowpark import Session
from snowflake.snowpark.types import StructType, StructField, StringType, IntegerType, DateType, BooleanType, VariantType
from snowflake.snowpark.functions import (
    col, coalesce, lit, lag, to_date, when, datediff, sum as s_sum, max as s_max, min as s_min, row_number,
    abs as s_abs, iff, least, call_function
)
from snowflake.snowpark.window import Window

# data pull - connect to snowflake
path_to_config = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))) + '\.config.json'
with open(path_to_config,"r") as f:
    config = json.load(f)
    connect_params = {
        "account": config["snowflake-deid-api"]["acct"],
        "user": config["snowflake-deid-api"]["user"],
        "password":config["snowflake-deid-api"]["pwd"],
        "role": config["snowflake-deid-api"]["role"],
        "warehouse": config["snowflake-deid-api"]["wh"]
    }

with Session.builder.configs(connect_params).create() as session:
    # test connection
    # print(session.sql("SELECT CURRENT_USER(), CURRENT_ROLE(), CURRENT_DATABASE()").collect())

    # set up session
    session.use_database(config["snowflake-deid-api"]["db"])
    session.use_schema("SX_CISTEM2")

    ##--- create table references
    log_tbl_dxpx = session.table("KTX_DXPX_LONG")
    log_tbl_rx = session.table("KTX_RX_LONG")

    ##--- identify ktx index date (retain site)
    w = Window.partitionBy(col("PATID")).orderBy(col("CD_DATE"))
    ktx_idx = (
        log_tbl_dxpx.filter(col('PHE_TYPE').isin(['KTx']))
            .filter(col('ENC_TYPE').isin(['EI','IP']))
            .filter(
                (col('CD_DATE') >= to_date(lit('2014-01-01'))) & 
                (col('CD_DATE') <= to_date(lit('2023-12-31')))
            )
            .with_column("rn", row_number().over(w))
            .filter(col("rn") == 1)
            .with_column_renamed("CD_DATE", "KTX_DATE1")
            .select("PATID", "KTX_DATE1", "SITE")
    )
    # ktx_idx.write.mode("overwrite").save_as_table("KTX_IDX")

    ##--- identify NODAT
    dm_idx = (
        log_tbl_dxpx.filter(col('PHE_TYPE').isin(['T2DM']))
            .filter(
                (col('CD_DATE') >= to_date(lit('2014-01-01'))) & 
                (col('CD_DATE') <= to_date(lit('2023-12-31')))
            )
            .group_by(col('PATID'))
            .agg(s_min(col('CD_DATE')).alias('DM_DATE1'))
    )

    nodat = (
        ktx_idx.join(dm_idx, ktx_idx["PATID"] == dm_idx['PATID'], "inner")
            .filter(dm_idx['DM_DATE1'] > ktx_idx['KTX_DATE1'])
            .select(
                ktx_idx['PATID'].alias("PATID"),
                dm_idx['DM_DATE1'].alias("DM_DATE1"),
                datediff("day", ktx_idx["KTX_DATE1"], dm_idx["DM_DATE1"]).alias("DAYS_TO_NODAT")
            )
    )
    # nodat.write.mode("overwrite").save_as_table("NODAT")

    ##--- identify MI
    mi_any = (
        log_tbl_dxpx.filter(col('PHE_TYPE').isin(['MI']))
            .filter(
                (col('CD_DATE') >= to_date(lit('2014-01-01'))) & 
                (col('CD_DATE') <= to_date(lit('2023-12-31')))
            )
            .filter(col('ENC_TYPE').isin(['EI','IP']))
    )

    mi_ae = (
        ktx_idx.join(mi_any, ktx_idx["PATID"] == mi_any["PATID"])
            .filter(mi_any["CD_DATE"] > ktx_idx["KTX_DATE1"])
            .select(
                ktx_idx["PATID"].alias("PATID"),
                mi_any["CD_DATE"].alias("MI_DATE1"),
                datediff("day", ktx_idx["KTX_DATE1"], mi_any["CD_DATE"]).alias("DAYS_TO_MI")
            )
            .group_by(ktx_idx["PATID"])
            .agg(s_min(mi_any["CD_DATE"]).alias("MI_DATE1"),s_min(col("DAYS_TO_MI")).alias("DAYS_TO_MI"))
    )
    # mi_ae.write.mode("overwrite").save_as_table("MI_AE")

    ##--- identify AR
    biopsy_idx = (
        ktx_idx.join(
            log_tbl_dxpx,
            (ktx_idx["PATID"]==log_tbl_dxpx["PATID"]) &
            (log_tbl_dxpx["PHE_TYPE"]).isin(['RenalBiopsy']) &
            (log_tbl_dxpx["CD_DATE"] >= to_date(lit("2014-01-01"))) &
            (log_tbl_dxpx["CD_DATE"] <= to_date(lit("2023-12-31"))) &
            (datediff("day",ktx_idx["KTX_DATE1"],log_tbl_dxpx["CD_DATE"]).between (0,180)),
            join_type = "inner"
        )
        .group_by(ktx_idx["PATID"],ktx_idx["KTX_DATE1"])
        .agg(s_min(log_tbl_dxpx["CD_DATE"]).alias("CD_DATE"))
        .select(
            ktx_idx["PATID"].alias("PATID"),
            log_tbl_dxpx["CD_DATE"].alias("RBX_DATE1"),
            datediff("day", ktx_idx["KTX_DATE1"], log_tbl_dxpx["CD_DATE"]).alias("DAYS_TO_RBX")
        )
    )
    ar_ae = (
        biopsy_idx.join(
            log_tbl_rx,
            (biopsy_idx["PATID"] == log_tbl_rx["PATID"]) & 
            (datediff("day",biopsy_idx["RBX_DATE1"],log_tbl_rx["CD_DATE"]).between(0,7)),
            "inner"
        )
        .group_by(biopsy_idx["PATID"],biopsy_idx["RBX_DATE1"],biopsy_idx["DAYS_TO_RBX"])
        .agg(s_min(log_tbl_rx["CD_DATE"]).alias("ANTIREJ_DATE1"))
        .select(
            biopsy_idx["PATID"].alias("PATID"),
            biopsy_idx["RBX_DATE1"].alias("RBX_DATE1"),
            biopsy_idx["DAYS_TO_RBX"].alias("DAYS_TO_RBX"),
            col("ANTIREJ_DATE1")
        )
    )
    # ar_ae.write.mode("overwrite").save_as_table("AR_AE")
    
    #--- join tables together
    k = ktx_idx.alias("k")      
    n = nodat.alias("n")        
    m = mi_ae.alias("m")        
    a = ar_ae.alias("a")
    p = session.table("PAT_TABLE1")

    jn1 = (
        k.join(n, k["PATID"] == n["PATID"], "left")
        .select(
            k["PATID"].alias("PATID"),
            k["KTX_DATE1"].alias("KTX_DATE1"),
            k["SITE"].alias("KTX_SITE"),
            n["DM_DATE1"].alias("DM_DATE1"),
            n["DAYS_TO_NODAT"].alias("DAYS_TO_NODAT")
        )
        .with_column("NODAT_IND", when(col("DM_DATE1").is_not_null(), 1).otherwise(0))
    )
    jn2 = (
        jn1.join(m, jn1["PATID"] == m["PATID"], "left")
            .select(
                jn1["PATID"],
                jn1["KTX_DATE1"],
                jn1["KTX_SITE"],
                jn1["DM_DATE1"],
                jn1["DAYS_TO_NODAT"],
                jn1["NODAT_IND"],
                m["MI_DATE1"],
                m["DAYS_TO_MI"]
            )
            .with_column("MI_IND", when(col("MI_DATE1").is_not_null(), 1).otherwise(0))
    )
    jn3 = (
        jn2.join(a, jn2["PATID"] == a["PATID"], "left")
            .select(
                jn2["PATID"],
                jn2["KTX_DATE1"],
                jn2["KTX_SITE"],
                jn2["DM_DATE1"],
                jn2["DAYS_TO_NODAT"],
                jn2["NODAT_IND"],
                jn2["MI_DATE1"],
                jn2["DAYS_TO_MI"],
                jn2["MI_IND"],
                a["RBX_DATE1"],
                a["DAYS_TO_RBX"],
                a["ANTIREJ_DATE1"],
                datediff("day",a["RBX_DATE1"],a["ANTIREJ_DATE1"]).alias("DAYS_RBX_TO_ANTIREJ"),
            )
            .with_column("AR_IND", when(col("RBX_DATE1").is_not_null(), 1).otherwise(0))
    )
    final = (
        jn3.join(p, jn3["PATID"] == p["PATID"], "inner")
            .select(
                jn3["PATID"].alias("PATID"),
                jn3["KTX_DATE1"].alias("INDEX_DATE"),
                jn3["KTX_SITE"],
                jn3["DM_DATE1"],
                jn3["DAYS_TO_NODAT"],
                jn3["NODAT_IND"],
                jn3["MI_DATE1"],
                jn3["DAYS_TO_MI"],
                jn3["MI_IND"],
                jn3["RBX_DATE1"],
                jn3["DAYS_TO_RBX"],
                jn3["ANTIREJ_DATE1"],
                jn3["DAYS_RBX_TO_ANTIREJ"],
                jn3["AR_IND"],
                p["SEX"],
                p["RACE"],
                p["HISPANIC"],
                datediff("year",p["BIRTH_DATE"],jn3["KTX_DATE1"]).alias("AGE_AT_KTX"),
                p["DEATH_IND"],
                p["CENSOR_DATE"],
                datediff("day",jn3["KTX_DATE1"],p["CENSOR_DATE"]).alias("DAYS_TO_CENSOR"),
                p["INDEX_SRC"].alias("SRC_SITE")
            )
    )
    final.write.mode("overwrite").save_as_table("KTX_TBL1")