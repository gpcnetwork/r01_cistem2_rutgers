import os
import json
from utils import QueryFromJson
from snowflake.snowpark import Session
from snowflake.snowpark.types import StructType, StructField, StringType, IntegerType, DateType, BooleanType, VariantType
from snowflake.snowpark.functions import (
    col, coalesce, lit, lag, to_date, when, datediff, sum as s_sum, max as s_max, min as s_min, row_number,
    abs as s_abs, iff, least, call_function
)

# metadata pull - no need to connect to snowflake
# vs_kd_px = QueryFromJson(
#     url = './ref/vs-cde-kd.json',
#     sqlty = 'snow',
#     cd_field = 'PX',
#     cdtype_field = 'PX_TYPE',
#     date_fields = ["PX_DATE",'ADMIT_DATE'],
#     srctbl_name = "DEIDENTIFIED_PCORNET_CDM.CDM.DEID_PROCEDURES",
#     other_fields=["PATID","ENCOUNTERID","ENC_TYPE"],
#     sel_keys = ['KTx','RenalBiopsy'],
#     sel_domain = "px"
# )
# print(vs_kd_px.gen_qry())

# vs_kd_dx = QueryFromJson(
#     url = './ref/vs-cde-kd.json',
#     sqlty = 'snow',
#     cd_field = 'DX',
#     cdtype_field = 'DX_TYPE',
#     date_fields = ["DX_DATE",'ADMIT_DATE'],
#     srctbl_name = "DEIDENTIFIED_PCORNET_CDM.CDM.DEID_DIAGNOSIS",
#     other_fields=["PATID","ENCOUNTERID","ENC_TYPE"],
#     sel_keys = ['MI','AR','T2DM'],
#     sel_domain = "dx"
# )
# print(vs_kd_dx.gen_qry())

# vs_kd_rx = QueryFromJson(
#     url = './ref/vs-cde-kd.json',
#     sqlty = 'snow',
#     cd_field = 'RXNORM_CUI',
#     date_fields = ["RX_START_DATE",'RX_ORDER_DATE'],
#     srctbl_name = "DEIDENTIFIED_PCORNET_CDM.CDM.DEID_PRESCRIBING",
#     other_fields=["PATID","ENCOUNTERID"],
#     sel_keys = ['AntiRejectionRx'],
#     sel_domain = "rx"
# )
# print(vs_kd_rx.gen_qry())

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

# Connecting to snowflake
site_lst = [
    "ALLINA",
    "IHC",
    "KUMC",
    "MCRI",
    "MCW",
    "MU",
    "UCD",
    "UIOWA",
    "UNMC",
    "UTHOUSTON",
    "UTHSCSA",
    "UTSW",
    "UU",
    "WASHU"
]
with Session.builder.configs(connect_params).create() as session:
    # test connection
    # print(session.sql("SELECT CURRENT_USER(), CURRENT_ROLE(), CURRENT_DATABASE()").collect())

    # set up session
    session.use_database(config["snowflake-deid-api"]["db"])
    session.use_schema("SX_CISTEM2")

    ##--- create long table shells
    log_tbl_dxpx = "KTX_DXPX_LONG" 
    schema = StructType([
        StructField("PATID", StringType()),
        StructField("ENCOUNTERID", StringType()),
        StructField("ENC_TYPE", StringType()),
        StructField("CD", StringType()),
        StructField("CD_TYPE", StringType()),
        StructField("CD_DATE", DateType()),
        StructField("PHE_TYPE", StringType()),
        StructField("SITE", StringType())
    ])
    empty_df1 = session.create_dataframe([], schema=schema)
    empty_df1.write.mode("overwrite").save_as_table(log_tbl_dxpx)

    log_tbl_rx = "KTX_RX_LONG" 
    schema = StructType([
        StructField("PATID", StringType()),
        StructField("ENCOUNTERID", StringType()),
        StructField("CD", StringType()),
        StructField("CD_TYPE", StringType()),
        StructField("CD_DATE", DateType()),
        StructField("PHE_TYPE", StringType()),
        StructField("SITE", StringType())
    ])
    empty_df2 = session.create_dataframe([], schema=schema)
    empty_df2.write.mode("overwrite").save_as_table(log_tbl_rx)

    ##--- event logging (logitudinal stacking)
    for s in site_lst:
        #--- collect dx, px
        vs_kd_px = QueryFromJson(
            url = './ref/vs-cde-kd.json',
            sqlty = 'snow',
            cd_field = 'PX',
            cdtype_field = 'PX_TYPE',
            date_fields = ["PX_DATE",'ADMIT_DATE'],
            srctbl_name = f"GROUSE_DEID_DB.PCORNET_CDM_{s}.V_DEID_PROCEDURES",
            other_fields=["PATID","ENCOUNTERID","ENC_TYPE"],
            sel_keys = ['KTx','RenalBiopsy'],
            sel_domain = "px"
        )
        vs_kd_dx = QueryFromJson(
            url = './ref/vs-cde-kd.json',
            sqlty = 'snow',
            cd_field = 'DX',
            cdtype_field = 'DX_TYPE',
            date_fields = ["DX_DATE",'ADMIT_DATE'],
            srctbl_name = f"GROUSE_DEID_DB.PCORNET_CDM_{s}.V_DEID_DIAGNOSIS",
            other_fields=["PATID","ENCOUNTERID","ENC_TYPE"],
            sel_keys = ['MI','AR','T2DM'],
            sel_domain = "dx"
        )
        ktx_qry_dxpx = ' UNION ALL'.join([
            vs_kd_px.gen_qry(),
            vs_kd_dx.gen_qry()
        ])
        insert_tmp_sql = f"""
        INSERT INTO TMP_{log_tbl_dxpx} (PATID,ENCOUNTERID,ENC_TYPE,CD,CD_TYPE,CD_DATE,PHE_TYPE)
            {ktx_qry_dxpx}
        """
        insert_sql = f"""
        INSERT INTO {log_tbl_dxpx} (PATID,ENCOUNTERID,ENC_TYPE,CD,CD_TYPE,CD_DATE,PHE_TYPE,SITE)
            SELECT PATID,ENCOUNTERID,ENC_TYPE,CD,CD_TYPE,CD_DATE,PHE_TYPE,'{s}' AS SITE FROM TMP_{log_tbl_dxpx}
        """
        empty_df1.drop("SITE").write.mode("overwrite").save_as_table(f"TMP_{log_tbl_dxpx}",table_type="temporary")
        session.sql(insert_tmp_sql).collect()
        session.sql(insert_sql).collect()

        #--- collect rx
        vs_kd_rx = QueryFromJson(
            url = './ref/vs-cde-kd.json',
            sqlty = 'snow',
            cd_field = 'RXNORM_CUI',
            date_fields = ["RX_START_DATE",'RX_ORDER_DATE'],
            srctbl_name = f"GROUSE_DEID_DB.PCORNET_CDM_{s}.V_DEID_PRESCRIBING",
            other_fields=["PATID","ENCOUNTERID"],
            sel_keys = ['AntiRejectionRx'],
            sel_domain = "rx"
        )
        empty_df2.drop("SITE").write.mode("overwrite").save_as_table(f"TMP_{log_tbl_rx}",table_type="temporary")
        insert_tmp_sql = f"""
        INSERT INTO TMP_{log_tbl_rx} (PATID,ENCOUNTERID,CD,CD_TYPE,CD_DATE,PHE_TYPE)
            {vs_kd_rx.gen_qry()}
        """
        insert_sql = f"""
        INSERT INTO {log_tbl_rx} (PATID,ENCOUNTERID,CD,CD_TYPE,CD_DATE,PHE_TYPE,SITE)
            SELECT PATID,ENCOUNTERID,CD,CD_TYPE,CD_DATE,PHE_TYPE,'{s}' AS SITE FROM TMP_{log_tbl_rx} 
        """
        session.sql(insert_tmp_sql).collect()
        session.sql(insert_sql).collect()





   
        