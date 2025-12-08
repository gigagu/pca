    
{{ config(
    alias='sub_ldgr',
    materialized='table'
) }}

 -- 

 SELECT
  ebbs_dsa_slpd_bs.country_code AS CTRY_CD,
  'EBBS' AS SRC_SYS,
  ebbs_dsa_slpd_bs.ods AS STRT_DT,
  '9999-12-31' AS END_DT,
  CASE
    WHEN ebbs_dsa_slpd_bs.fieldname33 = 'BC'
    THEN CONCAT(ebbs_dsa_slpd_bs.ba1_obj_curr, '-', ebbs_dsa_slpd_bs.bic_c_refid)
    WHEN ebbs_dsa_slpd_bs.fieldname33 = 'BD'
    THEN CONCAT(ebbs_dsa_slpd_bs.ba1_obj_curr, '-', ebbs_dsa_slpd_bs.fieldname20)
    WHEN ebbs_dsa_slpd_bs.fieldname33 = 'TC'
    THEN CONCAT(ebbs_dsa_slpd_bs.ba1_obj_curr, '-', ebbs_dsa_slpd_bs.fieldname20)
    WHEN ebbs_dsa_slpd_bs.fieldname33 = 'TD'
    THEN CONCAT(ebbs_dsa_slpd_bs.fieldname34, '-', ebbs_dsa_slpd_bs.fieldname20)
    WHEN ebbs_dsa_slpd_bs.fieldname33 = 'JL'
    THEN CONCAT(
      ebbs_dsa_slpd_bs.fieldname34,
      '-',
      COALESCE(ebbs_dsa_slpd_bs.fieldname20, ebbs_dsa_slpd_bs.bic_c_refid)
    )
  END AS SUBL_ID,
  CONCAT(ebbs_dsa_slpd_bs.ba1_c55contid, '|', ba1_c55docnum) AS SRC_TECH_ID,
  ebbs_dsa_slpd_bs.bic_c_tpportf AS PTFOL_NM,
  COALESCE(ebbs_dsa_slpd_bs.ba1_c55slacur, ebbs_dsa_slpd_bs.ba1_obj_curr) AS TRAN_CCY,
  ebbs_dsa_slpd_bs.bic_c_psglbu AS GL_BIZ_UNIT_CD,
  ebbs_dsa_slpd_bs.bic_c_psglac AS GL_ACCT_CD,
  ebbs_dsa_slpd_bs.bic_c_psgldi AS GL_DEPT_CD,
  ebbs_dsa_slpd_bs.bic_c_psglaf AS GL_AFFL_CD,
  ebbs_dsa_slpd_bs.bic_c_psglcc AS GL_CUST_CLAS_CD,
  ebbs_dsa_slpd_bs.bic_c_psglou AS GL_OP_UNIT_CD,
  ebbs_dsa_slpd_bs.bic_c_psglpr AS GL_PRD_CD,
  CONCAT(
    'EBBS-',
    CASE
      WHEN ebbs_dsa_slpd_bs.fieldname33 = 'BC'
      THEN CONCAT(
        COALESCE(ebbs_dsa_slpd_bs.ba1_obj_curr, 'NA'),
        '-',
        COALESCE(ebbs_dsa_slpd_bs.bic_c_refid, 'NA')
      )
      WHEN ebbs_dsa_slpd_bs.fieldname33 = 'BD'
      THEN CONCAT(
        COALESCE(ebbs_dsa_slpd_bs.ba1_obj_curr, 'NA'),
        '-',
        COALESCE(ebbs_dsa_slpd_bs.fieldname20, 'NA')
      )
      WHEN ebbs_dsa_slpd_bs.fieldname33 = 'TC'
      THEN CONCAT(
        COALESCE(ebbs_dsa_slpd_bs.ba1_obj_curr, 'NA'),
        '-',
        COALESCE(ebbs_dsa_slpd_bs.fieldname20, 'NA')
      )
      WHEN ebbs_dsa_slpd_bs.fieldname33 = 'TD'
      THEN CONCAT(
        COALESCE(ebbs_dsa_slpd_bs.fieldname34, 'NA'),
        '-',
        COALESCE(ebbs_dsa_slpd_bs.fieldname20, 'NA')
      )
      WHEN ebbs_dsa_slpd_bs.fieldname33 = 'JL'
      THEN CONCAT(
        COALESCE(ebbs_dsa_slpd_bs.fieldname34, 'NA'),
        '-',
        COALESCE(ebbs_dsa_slpd_bs.fieldname20, ebbs_dsa_slpd_bs.bic_c_refid)
      )
    END,
    '|',
    COALESCE(ebbs_dsa_slpd_bs.bic_c_psglbu, 'NA'),
    '|',
    COALESCE(ebbs_dsa_slpd_bs.bic_c_psglac, 'NA'),
    '|',
    COALESCE(ebbs_dsa_slpd_bs.bic_c_psglpr, 'NA'),
    '|',
    COALESCE(ebbs_dsa_slpd_bs.bic_c_psgldi, 'NA'),
    '|',
    COALESCE(ebbs_dsa_slpd_bs.bic_c_psglaf, 'NA'),
    '|',
    COALESCE(ebbs_dsa_slpd_bs.bic_c_psglcc, 'NA'),
    '|',
    COALESCE(ebbs_dsa_slpd_bs.bic_c_psglou, 'NA')
  ) AS UNIQ_ID,
  ebbs_dsa_slpd_bs.ba1_c55alst AS ACCT_CAT_NM,
  ebbs_dsa_slpd_bs.bic_c_acclass AS COA_ACCT_CLAS,
  ebbs_dsa_slpd_bs.g_dm_amt_group AS ACCT_SUB_CAT_NM,
  ebbs_dsa_slpd_bs.fieldname33 AS GL_AMT_TYPE,
  ebbs_dsa_slpd_bs.ba1_c55alst AS ASSET_LIAB_STS,
  ebbs_dsa_slpd_bs.ba1_bil_curr AS BASE_CCY,
  NULL AS CHRG_INT_CD,
  NULL AS CTRY_LVL_ID,
  ebbs_dsa_slpd_bs.bic_c_tpsrcsy AS DATA_SRC,
  NULL AS ECL_EXPSR_FLG,
  NULL AS GL_CUST_CLAS_CD_DESC,
  ebbs_dsa_slpd_bs.bic_c_psglexc AS GL_EXCLN_FLG,
  ebbs_dsa_slpd_bs.fieldname18 AS GL_PRD_CD_DESC,
  ebbs_dsa_slpd_bs.ba1_grp_curr AS GRP_CCY,
  NULL AS IFRS_SCHED,
  NULL AS INTCO_FLG,
  ebbs_dsa_slpd_bs.ba1_c55lgent AS LGL_ENT_CD,
  ebbs_dsa_slpd_bs.ba1_loc_curr AS LOCAL_CCY,
  NULL AS MTD_POSTED_AMT_BASE_CCY,
  NULL AS MTD_POSTED_AMT_TRAN_CCY,
  NULL AS NOT_AMT_CCY_CD1,
  NULL AS NOT_AMT_CCY_CD2,
  NULL AS NOT_AMT1,
  NULL AS NOT_AMT2,
  COALESCE(ebbs_dsa_slpd_bs.bic_c_cptype, ebbs_dsa_slpd_bs.bic_c_psglcpt) AS GL_CP_TYPE,
  ebbs_dsa_slpd_bs.bic_c_s4oploc AS S4_OPR_LOC_CD,
  ebbs_dsa_slpd_bs.bic_c_s4pc AS S4_PRD_CD,
  ebbs_dsa_slpd_bs.bic_c_s4pfcen AS S4_PRFT_CTR,
  ebbs_dsa_slpd_bs.bic_c_s4segmt AS S4_SGMT_CD,
  CASE
    WHEN ebbs_dsa_slpd_bs.fieldname33 = 'BC'
    THEN 'Account'
    WHEN ebbs_dsa_slpd_bs.fieldname33 = 'BD'
    THEN 'Deal'
    WHEN ebbs_dsa_slpd_bs.fieldname33 = 'TC'
    THEN 'Account Accrual'
    WHEN ebbs_dsa_slpd_bs.fieldname33 = 'TD'
    THEN 'Deal Accrual'
    WHEN ebbs_dsa_slpd_bs.fieldname33 = 'JL'
    THEN 'Other Internal Accounts'
    ELSE ''
  END AS SUBL_ID_TYPE,
  ebbs_dsa_slpd_bs.ba1_c55slacc AS SUBL_ACCT,
  ebbs_dsa_slpd_bs.sla_description AS SUBL_ACCT_DESC,
  ebbs_dsa_slpd_bs.sla_group AS SUBL_ACCT_GRP,
  ebbs_dsa_slpd_bs.sla_group_description AS SUBL_ACCT_GRP_DESC,
  ebbs_dsa_slpd_bs.ba1_k5samobj AS BAL_AMT,
  ebbs_dsa_slpd_bs.ba1_k5samgrp AS BAL_AMT_USD,
  NULL AS TRD_LEG_NUM,
  ebbs_dsa_slpd_bs.ba1_k5sambal AS YTD_POSTED_AMT_BASE_CCY,
  ebbs_dsa_slpd_bs.ba1_k5samgrp AS YTD_POSTED_AMT_GRP_CCY,
  ebbs_dsa_slpd_bs.ba1_k5samobj AS YTD_POSTED_AMT_TRAN_CCY,
  NULL AS GL_ACCT_FLG,
  NULL AS GL_ITD_AMT,
  NULL AS MKT_VAL_IN_USD_1,
  NULL AS MKT_VAL_IN_USD_2,
  NULL AS LEAD_TRD_FLG,
  ebbs_dsa_slpd_bs.bic_c_repct AS TRD_BK_CD,
  COALESCE(ebbs_dsa_slpd_bs.bic_c_psglcpt, ebbs_dsa_slpd_bs.bic_c_cptype) AS IFRS_CP_TYPE_CD,
  ebbs_dsa_slpd_bs.bic_c_type AS GL_PRD_TYPE,
  ebbs_dsa_slpd_bs.bic_c_tpsrcsy AS TP_SRC_SYS,
  m_gl_product_xlate.sci_std_prod_code AS GL_STD_PRD_CD,
  ebbs_dsa_slpd_bs.ods AS ODS
FROM {{ source('sri_open_schema', 'ebbs_dsa_slpd_bs') }} AS ebbs_dsa_slpd_bs
LEFT OUTER JOIN {{ source('sri_open_schema', 'm_gl_product_xlate') }} AS m_gl_product_xlate
  ON m_gl_product_xlate.psgl_prod_code = ebbs_dsa_slpd_bs.bic_c_psglpr
  AND m_gl_product_xlate.ods = '{{ var('__ods_val__') }}'
WHERE
  ebbs_dsa_slpd_bs.ods = '{{ var('__ods_val__') }}' AND ebbs_dsa_slpd_bs.tp_sys = 'EBBS'
 