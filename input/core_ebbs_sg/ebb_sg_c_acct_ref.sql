;EBB~SG~EBBS_SG_ACCOUNT~C_ACCT_REF~1400302SGEBBSELECT
concat('@DATA_SRC_EBB@', '-', '@SOURCE_COUNTRY_CODE@', '-', trim(account.CURRENCYCODE), '-', trim(account.ACCOUNTNO)) as ACCT_SROGT_ID
,concat(trim(account.CURRENCYCODE), '-', trim(account.ACCOUNTNO)) as DOM_ACCT_REF
,acodinfo.CURRENTREPAYMENTSTATUS as REPYMT_STS_CD1
,refmap.REPYMT_STS_DESC1 as REPYMT_STS_DESC1
,'EBB~SG~EBBS_SG_ACCOUNT~C_ACCT_REF~1' as PROCESS_ID
,from_unixtime(unix_timestamp()) as PPN_DTM
,'@DT_VRSN@' as DT_VRSN
,'@ACCS_CTRY_CD@' as ACCS_CTRY_CD
,w_prty_mast.ACCS_SEGMT_CD as ACCS_SEGMT_CD
,'@MNTHEND_FL@' as MNTHEND_FL
,account.DRINTINDFLAG as DR_INT_ACR_FL
FROM (SELECT * 
FROM @SOURCE_EBB@.EBBS_SG_ACCOUNT WHERE ods = '@ODS_EBB@') account
LEFT JOIN
(SELECT currentrepaymentstatus, accountno, currencycode 
FROM @SOURCE_EBB@.EBBS_SG_ACODINFO WHERE ods = '@ODS_EBB@') acodinfo
ON trim(account.ACCOUNTNO) = trim(acodinfo.ACCOUNTNO) AND trim(account.CURRENCYCODE) = trim(acodinfo.CURRENCYCODE)
LEFT JOIN
(SELECT masterno, relationshipno FROM @SOURCE_EBB@.EBBS_SG_MASTREL WHERE ods = '@ODS_EBB@' AND primaryflag = 'Y') mastrel
ON trim(account.MASTERNO) = trim(mastrel.MASTERNO)
LEFT JOIN
(SELECT prty_srogt_id, accs_segmt_cd FROM @TARGET_WORK@.C_CRTD_CORE_PRTY
WHERE process_date = '@PSE_DATE@' AND data_src = '@DATA_SRC_EBB@' AND source_country_code = '@SOURCE_COUNTRY_CODE@') w_prty_mast
ON concat('@DATA_SRC_EBB@', '-', '@SOURCE_COUNTRY_CODE@', '-', trim(mastrel.RELATIONSHIPNO)) = w_prty_mast.PRTY_SROGT_ID
LEFT JOIN
(SELECT dom_desc as repymt_sts_desc1, dom_cd FROM @SOURCE_REF@.C_REF_DAT_MAP 
WHERE trim(source_country_code) = '@SOURCE_COUNTRY_CODE@' AND trim(data_src) = '@DATA_SRC_EBB@' AND trim(map_tp_cd) = 'RPSTS') refmap
ON trim(acodinfo.CURRENTREPAYMENTSTATUS) = trim(refmap.DOM_CD)c_acct_refmaster1003