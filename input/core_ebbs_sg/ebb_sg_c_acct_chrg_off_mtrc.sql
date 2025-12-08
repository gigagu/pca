;EBB~SG~EBBS_SG_ACCOUNT~C_ACCT_CHRG_OFF_MTRC~1400302SGEBBSELECT
concat('@DATA_SRC_EBB@', '-', '@SOURCE_COUNTRY_CODE@', '-', trim(account.CURRENCYCODE), '-', trim(account.ACCOUNTNO)) as ACCT_SROGT_ID
,concat('@DATA_SRC_EBB@', '-', '@SOURCE_COUNTRY_CODE@', '-', trim(account.PRODUCTCODE)) as PROD_SROGT_ID
,concat('@DATA_SRC_EBB@', '-', '@SOURCE_COUNTRY_CODE@', '-', trim(mastrel.RELATIONSHIPNO)) as PRTY_SROGT_ID
,concat(trim(account.CURRENCYCODE), '-', trim(account.ACCOUNTNO)) as DOM_ACCT_REF
,account.PRODUCTCODE as DOM_PROD_CD
,mastrel.RELATIONSHIPNO as DOM_PRTY_REF
,account.CURRENCYCODE as ACCT_CURY_CD
,prvinfo.TOTALCHARGEOFFAMT as CHRGD_OFF_AMT
,'EBB~SG~EBBS_SG_ACCOUNT~C_ACCT_CHRG_OFF_MTRC~1' as PROCESS_ID
,from_unixtime(unix_timestamp()) as PPN_DTM
,'@DT_VRSN@' as DT_VRSN
,'@ACCS_CTRY_CD@' as ACCS_CTRY_CD
,wpm.ACCS_SEGMT_CD as ACCS_SEGMT_CD
,'@MNTHEND_FL@' as MNTHEND_FL
FROM (SELECT * FROM @SOURCE_EBB@.EBBS_SG_ACCOUNT WHERE ods = '@ODS_EBB@') account
LEFT JOIN
(SELECT currencycode, totalchargeoffamt, referenceno FROM @SOURCE_EBB@.EBBS_SG_PRVINFO WHERE ods = '@ODS_EBB@') prvinfo
ON trim(account.CURRENCYCODE) = trim(prvinfo.CURRENCYCODE) and trim(account.ACCOUNTNO) = trim(prvinfo.REFERENCENO)
LEFT JOIN
(SELECT relationshipno, masterno FROM @SOURCE_EBB@.EBBS_SG_MASTREL WHERE ods = '@ODS_EBB@' AND primaryflag = 'Y') mastrel
ON trim(account.MASTERNO) = trim(mastrel.MASTERNO)
LEFT JOIN
(SELECT accs_segmt_cd, prty_srogt_id FROM @TARGET_WORK@.C_CRTD_CORE_PRTY
WHERE process_date = '@PSE_DATE@' AND data_src = '@DATA_SRC_EBB@' AND source_country_code ='@SOURCE_COUNTRY_CODE@') wpm
ON concat('@DATA_SRC_EBB@', '-', '@SOURCE_COUNTRY_CODE@', '-', trim(mastrel.RELATIONSHIPNO)) = wpm.PRTY_SROGT_IDc_acct_chrg_off_mtrcmaster10011