{{ config(
    alias='prty_rtng_ebbs_mastcrg',
    materialized='incremental'
    
) }}

SELECT
  TRIM(CAST(EBBS_MAST.MASTERNO AS VARCHAR)) AS PRTY_ID,
  'EBBS' AS SRC_SYS,
  'IN' AS CTRY_CD,
  CAST(EBBS_MAST.ods AS DATE) AS STRT_DT,
  CAST('9999-12-31' AS DATE) AS END_DT,
  CAST(NULL AS VARCHAR) AS PRTY_CR_RISK_RTNG_AGNCY_TYPE,
  '' AS PRTY_CR_RISK_RTNG_CCY,
  EBBS_MAST.CRGCODE AS PRTY_CR_RISK_RTNG_CD,
  'INTERNAL' AS PRTY_CR_RISK_RTNG_TYPE,
  'TPCustomerCreditGrade' AS RTNG_CAT_CD,
  '' AS PRTY_CR_RISK_RTNG_CD_DESC,
  CAST(EBBS_MAST.CRGEFFECTIVEDATE AS DATE) AS PRTY_CR_RISK_RTNG_STRT_DT,
  CAST(EBBS_MAST.ods AS DATE) AS ODS,
  	'2025-06-06' AS ods_val,
	  'IN' AS country_val,
	  'EBBS' AS tp_sys_val,   
	  'PRTY_RTNG-EBBS=MASTCRG' AS process_id_val,
	  'daily' AS frequency_val,
	  CAST(current_date as VARCHAR)  AS version_val
FROM {{ source('sri_open_schema', 'ebbs_in_mast') }} AS ebbs_mast
WHERE
  EBBS_MAST.ODS = '{{ var("__ods_val__") }}'