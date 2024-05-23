# Databricks notebook source
from pyspark.sql.functions import col, lit, concat_ws, expr

def sp_get_transformation_func_mapping_arrays(config_id, return_length=8):
    # Get raw columns
    raw_columns_df = spark.sql(f"""
        SELECT CONCAT_WS(',', COLLECT_LIST(CONCAT('[', source_column_name, ']'))) AS raw_columns
        FROM cfg.vw_raw_to_curated_column_mapping 
        WHERE config_id = {config_id} AND active_ind = 1 
        AND (derived_ind = 0 OR (derived_ind = 1 AND transformation_function_id IS NULL))
    """)
    raw_columns = raw_columns_df.collect()[0]['raw_columns']

    derived_columns_df = spark.sql(f"""
        SELECT CONCAT_WS(',', COLLECT_LIST(CONCAT('[', source_column_name, ']'))) AS raw_columns
        FROM cfg.vw_raw_to_curated_column_mapping 
        WHERE config_id = {config_id} AND active_ind = 1 
        AND derived_ind = 1 AND transformation_function_id IS NOT NULL
    """)
    derived_columns = derived_columns_df.collect()[0]['raw_columns']

    # Construct source query
    if len(derived_columns) >0:
        source_query_df = spark.sql(f"""
            SELECT CONCAT('SELECT ', '{raw_columns}, {derived_columns}', ' FROM ', source_database_name, '.', source_schema_name, '.', source_table_name, ' WHERE AUDIT_ACTIVE_ROW_IND=\\'Y\\'') AS source_query
            FROM cfg.source_to_output_config 
            WHERE config_id = {config_id}
        """)        
    else:    
        source_query_df = spark.sql(f"""
            SELECT CONCAT('SELECT ', '{raw_columns}', ' FROM ', source_database_name, '.', source_schema_name, '.', source_table_name, ' WHERE AUDIT_ACTIVE_ROW_IND=\\'Y\\'') AS source_query
            FROM cfg.source_to_output_config 
            WHERE config_id = {config_id}
        """)
    source_query = source_query_df.collect()[0]['source_query']

    # Fetch transformation function details
    transformation_functions_df = spark.sql(f"""
        SELECT source_column_name, 
               replace(transformation_function_def,'i0', source_column_name) AS f_definition, 
               transformation_parameter_text AS f_parameter_text, 
               transform_func_data_type AS transform_data_type
        FROM data_management.cfg.vw_raw_to_curated_column_mapping
        WHERE active_ind = 1 AND config_id = {config_id} AND transformation_function_id IS NOT NULL
        ORDER BY custom_column_mapping_id
    """)

    # Construct return keys and values for each data type
    ret_arrays = {}
    for row in transformation_functions_df.collect():
        column_name = row['source_column_name']
        f_definition = row['f_definition']
        p_list = row['f_parameter_text'].split('|')
        p_dtype = row['transform_data_type']
        
        ret_values = []
        for i, p in enumerate(p_list):
            ret_values.append(f'replace({f_definition}, \'i{i+1}\', \'{p}\')')

        if p_dtype == 'Boolean':
            ret_arrays.setdefault('bkeys', []).append(column_name)
            ret_arrays.setdefault('bvals', []).extend(ret_values)
        elif p_dtype == 'Date':
            ret_arrays.setdefault('dkeys', []).append(column_name)
            ret_arrays.setdefault('dvals', []).extend(ret_values)
        elif p_dtype in ['Timestamp', 'DateTime']:
            ret_arrays.setdefault('dtkeys', []).append(column_name)
            ret_arrays.setdefault('dtvals', []).extend(ret_values)
        elif p_dtype in ['Numeric', 'Decimal', 'Integer', 'Bit']:
            ret_arrays.setdefault('nkeys', []).append(column_name)
            ret_arrays.setdefault('nvals', []).extend(ret_values)
        elif p_dtype == 'String':
            ret_arrays.setdefault('skeys', []).append(column_name)
            ret_arrays.setdefault('svals', []).extend(ret_values)

    # Pad each variable to return_length
    for dtype in ['b', 'd', 'dt', 'n', 's']:
        count = len(ret_arrays.get(dtype + 'keys', []))
        for i in range(count, return_length):
            ret_arrays.setdefault(dtype + 'keys', []).append(f'_{dtype}{i+1}')
            ret_arrays.setdefault(dtype + 'vals', []).append(f'to{dtype.capitalize()}(null())')

    # Convert lists to string arrays
    for key, value in ret_arrays.items():
        ret_arrays[key] = ','.join([f'"{v}"' for v in value])

    return {
        'source_query': source_query,
        'bkeys': ret_arrays.get('bkeys', ''),
        'bvals': ret_arrays.get('bvals', ''),
        'dkeys': ret_arrays.get('dkeys', ''),
        'dvals': ret_arrays.get('dvals', ''),
        'dtkeys': ret_arrays.get('dtkeys', ''),
        'dtvals': ret_arrays.get('dtvals', ''),
        'nkeys': ret_arrays.get('nkeys', ''),
        'nvals': ret_arrays.get('nvals', ''),
        'skeys': ret_arrays.get('skeys', ''),
        'svals': ret_arrays.get('svals', '')
    }


# COMMAND ----------

sp_get_transformation_func_mapping_arrays(672,40)

# COMMAND ----------

config_id=672

raw_columns_df = spark.sql(f"""
    SELECT CONCAT('[', CONCAT_WS(',', COLLECT_LIST(CONCAT('[', source_column_name, ']'))), ']') AS raw_columns
    FROM cfg.vw_raw_to_curated_column_mapping 
    WHERE config_id = {config_id} AND active_ind = 1 
    AND (derived_ind = 0 OR (derived_ind = 1 AND transformation_function_id IS NULL))
""")

raw_columns = raw_columns_df.collect()[0]['raw_columns']

print(raw_columns)

# COMMAND ----------

raw_columns = raw_columns_df.collect()[0]['raw_columns']

sql_stmt=f"""
    SELECT CONCAT('SELECT ', '{raw_columns}', ' FROM ', source_database_name, '.', source_schema_name, '.', source_table_name, ' WHERE AUDIT_ACTIVE_ROW_IND=\\'Y\\'') AS source_query
    FROM cfg.source_to_output_config 
    WHERE config_id = {config_id}
"""
print(sql_stmt)
# # Construct source query
# source_query_df = spark.sql(f"""
#     SELECT CONCAT('SELECT ', '{raw_columns}', ' FROM ', source_database_name, '.', source_schema_name, '.', source_table_name, ' WHERE AUDIT_ACTIVE_ROW_IND=\'Y\'') AS source_query
#     FROM cfg.source_to_output_config 
#     WHERE config_id = {config_id}
# """)

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT CONCAT('SELECT ', '[[ACCTNO],[ACTYPE],[AUDIT_EFFECTIVE_START_DTS],[AUDIT_EFFECTIVE_END_DTS],[AUDIT_ROW_INSERT_DTS],[AUDIT_ROW_INSERT_JOB_ID],[AUDIT_ROW_UPDATE_JOB_ID],[AUDIT_ROW_UPDATE_DTS],[AUDIT_ACTIVE_CD],[AUDIT_ACTIVE_ROW_IND],[AUDIT_CDC_HASH_KEY],[BR#],[SNAME],[CIFNO],[ALTADD],[ADDNAM],[ALTNAM],[OFFCR],[COFFCR],[STATUS],[PRVSTS],[CLASS],[TYPE],[ORGAMT],[CBAL],[ACBAL],[QRYBAL],[SLDBAL],[ACCINT],[ACCIN2],[DLRACC],[PDIEM],[BKDISC],[DLRDSC],[OTHCHG],[ASSLC],[PAIDLC],[MINCG],[CURPO],[NXTPO],[INTREB],[DLRREB],[PMTAMT],[PIAMT],[LSTPMT],[PRTPMT],[BILPRN],[BILINT],[BILESC],[BILLC],[BILOC],[BILCL],[BILAH],[BILUN],[EOLDPM],[OLDPI],[OLDEPM],[NEWPMT],[NEWPI],[AMRSUS],[SUBAMT],[SUBREM],[AMRINT],[APRAMT],[ANINCM],[PREACC],[PREDSC],[YSCBAL],[YSACCI],[YSIREB],[YSDREB],[YSOTHC],[INTODY],[STMBAL],[RENBAL],[BALLON],[PACBAL],[CHGOFF],[PNTPD],[LSTADV],[CURTMN],[ACCURT],[SHCOPR],[SHCOIN],[SHIAP],[SHGLPR],[SHACCI],[SHCINT],[SHCSTS],[YRLFEE],[MINADV],[LNUCHG],[POAMNT],[ORGADV],[MTDADV],[MTDCURT],[MTDPRIN],[MTDCAPDR],[MTDCAPCR],[ORGDT6],[MATDT6],[DATOP6],[NREVD6],[COEXD6],[UCEXD6],[NSPDT6],[FPDT6],[LPDT6],[NPDT6],[NPDAY],[THRUD6],[PDTOD6],[CURED6],[CURBD6],[RENDT6],[RENPD6],[EXTDT6],[EXTPD6],[EXAMD6],[LFMDT6],[ORGMD6],[NWPMD6],[NWESD6],[NWEAD6],[NEAND6],[PREVD6],[STMDT6],[CPNDT6],[NACDT6],[CHGDT6],[WCHDT6],[PRVDAY],[APPDT6],[AMRDT6],[LSTAD6],[LSTCP6],[ECRDT6],[ECRDAY],[ENWRD6],[CVEXP6],[ACTIV6],[FRCLD6],[REAMD6],[REAMDAY],[DTPDO6],[CNVDT6],[PERMFD6],[HECNV6],[STEPRD6],[ORGADVD6],[REPRICD6],[ORGDT],[MATDT],[DATOPN],[NREVDT],[COEXDT],[UCEXDT],[NSPDT],[FPDT],[LPDT],[NPDT],[THRUDT],[PDTODT],[CUREDT],[CURBDT],[RENDT],[RENPDT],[EXTDT],[EXTPDT],[EXAMDT],[LFMDT],[ORGMDT],[NWPMDT],[NWESDT],[NWEADT],[NEANDT],[PREVDT],[STMDT],[CPNDT],[NACDT],[CHGDT],[WCHDT],[APPDT7],[AMRDT7],[LSTAD7],[LSTCP7],[ECRDT],[ENWRDT],[CVEXP7],[ACTIV7],[FRCLD7],[REAMD7],[DTPDO7],[CNVDT7],[PERMFD7],[HECNV7],[STEPRDT],[ORGADVD7],[REPRICD7],[RATE],[DLQRATE],[DLRATE],[DLRUPC],[DLRRPC],[DLRDYS],[DLRPMT],[DLRFOR],[DLRBOD],[DLRRSV],[DLRBRS],[DLRERN],[DLRBER],[DLRUER],[DLRFER],[TTLINT],[MATRAT],[PRATE#],[DLQRATE#],[PVARI],[PVCODE],[DLQVARI],[DLQVCODE],[DLQRTDYS],[PFLOOR],[PCEIL],[PRTERM],[PRCODE],[ECAPUP],[ECAPDN],[ECRTRM],[ECRTCD],[ECRYOV],[ECRYOS],[ECRNOV],[ECRNOS],[ECRPOV],[ECRPOS],[ECRYOU],[ERMETH],[ERAMT],[ECPRT],[EPRVRT],[ENWRT],[EPMCAP],[ENPDYS],[ORGIDX],[CURIDX],[PRVIDX],[FUTIDX],[CVRTAB],[CINDX#],[CVVARI],[CVVARC],[TERM],[TMCODE],[FREQ],[FRCODE],[SEMDY1],[SEMDY2],[IBASE],[PMTCOD],[ALTSCH],[REAMOR],[BILNDY],[REAMTRM],[REAMTMCD],[PFLC],[PFEXTF],[PFBAL],[PFEARN],[PFDAYS],[CPD10],[CPD30],[CPD60],[CPD90],[EXTFEE],[ORGFEE],[YTDINT],[YTDPRN],[YTDLC],[EXTYTD],[RTCYTD],[RENYTD],[YTDADV],[YTDCURT],[YTDPRIN],[YTDCAPDR],[YTDCAPCR],[PPD10],[PPD30],[PPD60],[PPD90],[INTPRE],[LCPRE],[IRSPRE],[EXTPRE],[ORGPRE],[RENPRE],[LPD10],[LPD30],[LPD60],[LPD90],[INTLTD],[EXTLTD],[RTCLTD],[EXTMTH],[EXTFEL],[RENLTD],[IRSLTD],[ORGLTD],[NEXLTD],[DELIN4],[DELIN3],[DELIN2],[DELIN1],[QAGQRYBAL],[PARTID],[CLCODE],[DEPTCD],[SHADOW],[SHACCF],[SHRPCO],[GROUP],[GLCOST],[GLPROD],[CALREP],[COLCOD],[PURCOD],[DLRNO],[HMAIL],[INCLUD],[DEMSTU],[INDCOD],[NAICSC],[ESCPSQ],[INTPSQ],[LTCPSQ],[OTHPSQ],[PRNPSQ],[INSPSQ],[USEBIL],[SENDCD],[SENDMT],[TITLE],[PNFLAG],[PHFLAG],[SENDPD],[PDCODE],[LCTYPE],[IGNLCP],[TYPCLS],[RATCOD],[CENSUS],[SMSA],[CRALOC],[STATE],[CRASTA],[COUNTY],[VIN#],[MSG],[RENEXT],[USER1],[USER2],[ESCROW],[CLSZRO],[COUPON],[CPNTRM],[HASARM],[ALMTYP],[PRTPCT],[PRPRPC],[PRINPC],[PRTSQ#],[PRTPMS],[PRT1PM],[FNDSQ#],[FNDLMT],[FNDPCT],[PCACC#],[PCACTP],[CLACC#],[TAXREF],[RLACC#],[RLACTP],[HDACC#],[HDACTP],[REWLN#],[RQ1098],[STMPAS],[MATGRC],[WCHCOD],[PPPCOD],[FLINRQ],[BRWPD],[RSTRUC],[SECURE],[LNPIC1],[LNPIC2],[LNPIC3],[LNPIC4],[LNPIC5],[LNPIC6],[LNPIC7],[LNPDT1],[LNPD61],[LNPDT2],[LNPD62],[LNPAM1],[LNPAM2],[LNPAM3],[LNPAM4],[LNPAM5],[LNPAM6],[LNPAM7],[LNANIC],[LTVRAT],[CLTVRT],[SLOUTS],[OLDMNY],[NEWMNY],[DWNPMT],[XCLSTT],[POPEND],[UNPADV],[REMTRM],[CRAMIL],[SCRACOD],[SCRABDATE],[SCRABDAT6],[SCRAEDATE],[SCRAEDAT6],[LNSPCC],[CBC10],[CBC30],[CBC60],[CBC90],[STM#AC],[RSTADV],[RSTPMT],[LNMCHK],[LNREGH],[LNSENL],[BADCK#],[HIRISK],[LNREGP],[RECEIT],[DLORAT],[NEGPCT],[NSFFEE],[MADVFE],[DEFINT],[PTDINT],[USRSTS],[STPADV],[STPAD6],[STPAD7],[STPPMT],[STPPM6],[STPPM7],[STPTAX],[STPTX6],[STPTX7],[STPHZD],[STPHZ6],[STPHZ7],[STPOPT],[STPOP6],[STPOP7],[STPLNS],[STPLS6],[STPLS7],[STPNOT],[STPNT6],[STPNT7],[STPCRP],[STPCR6],[STPCR7],[STPPRP],[STPPR6],[STPPR7],[STPACR],[STPAC6],[STPAC7],[STPCOR],[STPCO6],[STPCO7],[STPIRS],[STPIR6],[STPIR7],[STPSTM],[STPST6],[STPST7],[STPCOU],[STPCP6],[STPCP7],[STPESD],[STPED6],[STPED7],[STPESA],[STPEA6],[STPEA7],[STPPDP],[STPPD6],[STPPD7],[PSTSHR],[USEUNA],[HOWUNA],[DLRLOC],[DLRTYP],[DLRFDP],[DLRFDPC],[CDLRFOR],[CDLRDYS],[CDLRPMT],[CDLRPDD],[RECRSCD],[RECRSPC],[RECRSTM],[RECRSTC],[RECDAYP],[RECINT],[PARTID#],[PNFLAGS],[PAYEX6],[PAYEX7],[CPD120],[CPD150],[CPD180],[PPD120],[PPD150],[PPD180],[LPD120],[LPD150],[LPD180],[PCTREA],[PARTPC],[BILOLA],[YTDPEN],[PREPEN],[LTDPEN],[SUBPRM],[OVLFEE],[CLCIF#],[ADOTNB],[UNTPDLRFP],[MINUNTADV],[SYNDAGTNBR],[SYNDMBRNBR],[SHRNTLCRD],[CORRESBANK],[CRCYTYPE],[FRDALRT],[ARGPRDCOD],[CONSTR],[RGRISK],[TEASER],[CSPLTV],[EXSLTV],[EXRELP],[LNATMCRD],[HMDA],[SALESAC],[RTCCTR],[AFFILIATE],[MINBIL],[INTFIR],[STEPRT],[LNLANG],[APPDLNS],[NBRPDPMT],[APPPDLNS],[NPDAPPLT],[NPDPPLTR],[POSTEXPC],[SHDOW6],[SHDOW7],[RSHTPC6],[ODIBAS],[MTDADV#],[YRLCALC],[LOCCALC],[LOCFREQ],[LOCFRCD],[LOCFDT6],[LOCFDT7],[LOCFDY],[UNUCALC],[UNUFREQ],[UNUFRCD],[UNUFDT6],[UNUFDT7],[UNUFDY],[#DYSZROCUR],[#DYSZRO01],[#DYSZRO02],[#DYSZRO03],[#DYSZRO04],[#DYSZRO05],[#DYSZRO06],[#DYSZRO07],[#DYSZRO08],[#DYSZRO09],[#DYSZRO10],[#DYSZRO11],[#DYSZRO12],[LPTYPE],[CPASSLN],[RPASSLN],[RPASSPCT],[PPSDT6],[PPSDT7],[PPSFRQ],[PPSCOD],[PPDBAL],[PPCURT],[PPEXP6],[PPEXP7],[CURNTC6],[CURNTC7],[CUREXP6],[CUREXP7],[CURALL6],[CURALL7],[INVALL6],[INVALL7],[RECOUPASD],[RECOUPYTD],[RECOUPPRE],[NSFNTCYTD],[NSFNTCPRE],[PAYCURT],[PAYCOUP],[RECPRNDT6],[RECPRNDT7],[CURTAMT1],[CURTAMT2],[DATEPEN16],[DATEPEN17],[DATEPEN26],[DATEPEN27],[LNTIER],[LQBL#2],[LQRT#2],[LQBL#3],[LQRT#3],[LQBL#4],[LQRT#4],[LQBL#5],[LQRT#5],[LQBL#6],[LQRT#6],[LQBL#7],[LQRT#7],[LQBL#8],[LQRT#8],[LQBL#9],[LQRT#9],[LQBL#10],[LQRT#10],[LSPLIT],[LIENPOSN],[ODPUSEHOLD],[PENRAT#],[PENNEWRT],[PENRATE],[PENPDAYS],[PENAFTNT],[PENRVTM],[PENRVTMC],[PENNOTD6],[PENNOTD7],[PENSTDT6],[PENSTDT7],[PENRVDT6],[PENRVDT7],[PENRATYN],[PENLNRATE],[PPVARI],[PPVCODE],[SPECLEN],[TSAMT1],[TSAMT2],[TSDATE1],[TSDATE2],[TS1CHAR1],[TS1CHAR2],[TS2CHAR3],[TS2CHAR4],[PRMRSKRAT],[RSKRATDT6],[RSKRATDT7],[SPLRSKRAT1],[SPLRSKAMT1],[SPLRSKRAT2],[SPLRSKPCT2],[SPLRSKAMT2],[SPLRSKRAT3],[SPLRSKPCT3],[SPLRSKAMT3],[SPLRSKCA1],[NGAMORG$],[AUTOBIL],[YTDESC],[PRVESC],[YTDFEE],[PRVFEE],[PRVPRN],[PRIDWE],[ARMNTC],[INARMD],[ARMSNT],[EINDT6],[EINDT7]]', ' FROM ', source_database_name, '.', source_schema_name, '.', source_table_name, ' WHERE AUDIT_ACTIVE_ROW_IND=\'Y\'') AS source_query
# MAGIC     FROM cfg.source_to_output_config 
# MAGIC     WHERE config_id = 672

# COMMAND ----------

