    
{{ config(
    alias='chrt_of_acct1',
    materialized='table'
) }}

 -- 

 SELECT
  'RDM' AS SRC_SYS,
  rdm_all_pgc_mstr_acct.ods AS STRT_DT,
  CAST('9999-12-31' AS DATE) AS END_DT,
  NULL AS SET_ID,
  rdm_all_pgc_mstr_acct.pgcmasteraccountid AS CHRT_FLD_ID,
  CAST(rdm_all_pgc_mstr_acct.pgcmasteraccounteffectivedate AS DATE) AS EFF_DT,
  rdm_all_pgc_mstr_acct.pgcmasteraccounteffectivestatus AS EFF_STS,
  rdm_all_pgc_mstr_acct.pgcmasteraccountdesc AS DESC,
  NULL AS SHORT_DESC,
  'Account' AS CHRT_FLD_TYPE_CD,
  NULL AS ISLM_FLG,
  rdm_all_pgc_mstr_acct.pgcmastercategory AS CHRT_FLD_CAT_CD,
  NULL AS GRP_SOLO_FLG,
  NULL AS CHRT_FLD_CTRY_CD,
  NULL AS CHRT_FLD_LVL_0,
  NULL AS CHRT_FLD_LVL_0_DESC,
  NULL AS CHRT_FLD_LVL_1,
  NULL AS CHRT_FLD_LVL_1_DESC,
  rdm_all_pgc_mstr_acct.ifrsschedulereference AS PGC_IFRSS_SCH,
  rdm_all_pgc_mstr_acct.ifrsscheduledesc AS PGC_IFRSS_SCH_DESC,
  rdm_all_pgc_mstr_acct.pgcmastergroupindicator AS PGC_GRP_NONGRP,
  rdm_all_pgc_mstr_acct.mr_schedulereference AS PGC_MR_SCH_REF,
  NULL AS CTRY_CD,
  rdm_all_pgc_mstr_acct.ODS AS ODS,
  NULL AS CHRT_FLD_CCY
FROM {{ source('sri_open_schema', 'rdm_all_pgc_mstr_acct') }} AS rdm_all_pgc_mstr_acct
WHERE
  rdm_all_pgc_mstr_acct.ODS = '{{ var('__ods_val__') }}'
 