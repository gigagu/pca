;EBB~SG~EBBS_SG_ACCOUNT~C_ACCT~1400302SGEBBSELECT
CONCAT('@DATA_SRC_EBB@', '-', '@SOURCE_COUNTRY_CODE@', '-', trim(account.CURRENCYCODE), '-', trim(account.ACCOUNTNO)) as ACCT_SROGT_ID
,CONCAT(trim(account.CURRENCYCODE), '-', trim(account.ACCOUNTNO)) as DOM_ACCT_REF
,'CORE' as ACCT_CLAS
,'CASA' as ACCT_SUB_CLAS
,prd.PRDCATEGORY as ACCT_TP_CD
,prdct.ACCT_TP_DESC as ACCT_TP_DESC
,account.ACCTCURRENTSTATUS as ACCT_STS_TP_CD
,acsts.ACCT_STS_TP_DESC as ACCT_STS_TP_DESC
,account.ACOPENDATE as ACCT_OPEN_DT
,account.ACCLOSEDT as ACCT_CLSE_DT
,account.CURRENCYCODE as ACCT_CURY_CD
,CONCAT('@DATA_SRC_EBB@', '-', '@SOURCE_COUNTRY_CODE@', '-', trim(mastrel.RELATIONSHIPNO)) as PRTY_SROGT_ID
,mastrel.RELATIONSHIPNO as DOM_PRTY_REF
,mast.ARMCODE as ARM_CD
,CONCAT('@DATA_SRC_EBB@', '-', '@SOURCE_COUNTRY_CODE@', '-', trim(account.PRODUCTCODE)) as PROD_SROGT_ID
,account.PRODUCTCODE as DOM_PROD_CD
,CONCAT('@DATA_SRC_EBB@', '-', '@SOURCE_COUNTRY_CODE@', '-', trim(account.ACCOUNTBRANCH)) as ACCT_BRNCH_SROGT_ID
,account.ACCOUNTBRANCH as ACCT_BRNCH_CD
,account.INTERESTCODE as INT_CD
,account.TRANSTODORMANTDATE as DRMT_DT
,t.FREQUENCY as STMT_CYC_FREQ
,t.ACTUALSTMTDATE as CURR_STMT_DT
,prvinfo.CHARGEOFFDATE as CHRGD_OFF_DT
,mast.ACCTTYPE as INTN_OR_CUST_ACCT_IND
,account.TRANSTOUNCLAIMDATE as UNCLM_DT
,t.STMTTYPECODE as STMT_CD
,stmttyp.DESCRIPTION as STMT_DESC
,account.MASTERNO as DOM_MAST_REF
,t2.INFODETCODE as PAYR_IND
,mast.INSTCLASSCODE as INST_CLAS_CD
,instcls.DESCRIPTION as INST_CLAS_DESC
,instcls.ISSTAFFCLASS as SCB_STAF_FL
,mast.ISICCODE as ISIC_CD
,isic.DESCRIPTION as ISIC_DESC
,mast.CUSTSEGMTCODE as SEGMT_CD
,custseg.DESCRIPTION as SEGMT_DESC
,mast.SEGMENTCODE as SUB_SEGMT_CD
,seg.NAME as SUB_SEGMT_CD_DESC
,account.ACCLOSUREREASON as ACCT_CLSE_RESN_CD
,reason.DESCRIPTION as ACCT_CLSE_RESN_DESC
,'EBB~SG~EBBS_SG_ACCOUNT~C_ACCT~1' as PROCESS_ID
,from_unixtime(unix_timestamp()) as PPN_DTM
,'@DT_VRSN@' as DT_VRSN
,'@ACCS_CTRY_CD@' as ACCS_CTRY_CD
,w_prty_mast.ACCS_SEGMT_CD as ACCS_SEGMT_CD
,'@MNTHEND_FL@' as MNTHEND_FL
,account.DSRREFERRALID AS REFL_ID
,account.DSRSOURCINGID AS SRCG_ID
,account.DSRCLOSINGID AS CLSG_ID
----POET Changes v1.8 ---START
,account.OPERATINGINS as ACCT_OPRT_INSTR_CD
,trim(AOICD.ACCT_OPRT_INSTR_DESC) as ACCT_OPRT_INSTR_DESC
 ----POET Changes v1.8 ---END
,account.PROVISIONSTATUS as PROV_STS_CD
,account.CHECKERID as CHECKER_ID
,ACCOUNT.ATMFLAG AS ATM_TRAN_ALWD_FL
,ACCOUNT.CALENDERACOPNDT as CUST_ATCH_ACCT_DT
--LDC CHANGES 4.2 ---START
,ACCTINT.CRADDTOGRATEFLAG as ACCTLVL_CRINT_GLBL_RT_FL
,ACCTINT.DRADDTOGRATEFLAG as ACCTLVL_DRINT_GLBL_RT_FL
,intc.CRADDTOGRATEFLAG as PRODLVL_CRINT_GLBL_RT_FL
,intc.DRADDTOGRATEFLAG as PRODLVL_DRINT_GLBL_RT_FL
,ACCOUNT.CRINTPRODUCTCODE as CR_INT_PROD_CD
,ACCOUNT.DRINTPRODUCTCODE as DR_INT_PROD_CD
--LDC 4.2Changes --END


FROM (SELECT * FROM @SOURCE_EBB@.EBBS_SG_ACCOUNT where ods = '@ODS_EBB@') account
LEFT JOIN
(SELECT masterno, relationshipno, primaryflag FROM @SOURCE_EBB@.EBBS_SG_MASTREL where ods = '@ODS_EBB@' and primaryflag = 'Y') mastrel
ON trim(account.MASTERNO) = trim(mastrel.MASTERNO)
LEFT JOIN
(SELECT prty_srogt_id, accs_segmt_cd FROM @TARGET_WORK@.C_CRTD_CORE_PRTY 
WHERE process_date = '@PSE_DATE@' AND data_src = '@DATA_SRC_EBB@' AND source_country_code = '@SOURCE_COUNTRY_CODE@') w_prty_mast
ON CONCAT('@DATA_SRC_EBB@', '-', '@SOURCE_COUNTRY_CODE@', '-', trim(mastrel.RELATIONSHIPNO)) = w_prty_mast.PRTY_SROGT_ID
LEFT JOIN
(SELECT prdcategory, productcode FROM @SOURCE_EBB@.EBBS_SG_PRD where ods = '@ODS_EBB@') prd
ON trim(account.PRODUCTCODE) = trim(prd.PRODUCTCODE)
LEFT JOIN
(SELECT dom_desc as acct_tp_desc, dom_cd FROM @SOURCE_REF@.C_REF_DAT_MAP
WHERE trim(source_country_code) = '@SOURCE_COUNTRY_CODE@' and trim(data_src) = '@DATA_SRC_EBB@' and trim(map_tp_cd) = 'PRDCT') prdct
ON trim(prd.PRDCATEGORY) = trim(prdct.DOM_CD)
LEFT JOIN
(SELECT dom_desc as acct_sts_tp_desc, dom_cd FROM @SOURCE_REF@.C_REF_DAT_MAP 
WHERE trim(source_country_code) = '@SOURCE_COUNTRY_CODE@' and trim(data_src) = '@DATA_SRC_EBB@' and trim(map_tp_cd) = 'ACSTS') acsts
ON trim(account.ACCTCURRENTSTATUS) = trim(acsts.DOM_CD)
LEFT JOIN
(SELECT armcode, accttype, instclasscode, isiccode, custsegmtcode, segmentcode, masterno FROM @SOURCE_EBB@.EBBS_SG_MAST WHERE ods = '@ODS_EBB@') mast
ON trim(account.MASTERNO) = trim(mast.MASTERNO)
LEFT JOIN
(SELECT base.CURRENCYCODE, base.ACCOUNTNO, base.SEQNO, base.ACTUALSTMTDATE, base.FREQUENCY, base.STMTTYPECODE FROM
(SELECT
row_number() OVER (partition by accountno, currencycode order by seqno desc) as MAX_ROW,
currencycode,
accountno,
seqno,
actualstmtdate,
frequency,
stmttypecode
FROM (SELECT * FROM @SOURCE_EBB@.EBBS_SG_ACSTMT WHERE ods = '@ODS_EBB@') acstmt) base WHERE base.MAX_ROW = 1) t
ON trim(account.ACCOUNTNO) = trim(t.ACCOUNTNO) AND trim(account.CURRENCYCODE) = trim(t.CURRENCYCODE)
LEFT JOIN
(SELECT currencycode, referenceno, chargeoffdate FROM @SOURCE_EBB@.EBBS_SG_PRVINFO WHERE ods = '@ODS_EBB@') prvinfo
ON trim(account.ACCOUNTNO) = trim(prvinfo.REFERENCENO) AND trim(account.CURRENCYCODE) = trim(prvinfo.CURRENCYCODE)
LEFT JOIN
(SELECT base2.CURRENCYCODE, base2.ACCOUNTNO, base2.SEQNO, base2.INFODETCODE, base2.INFORMATIONCODE
FROM (SELECT currencycode, accountno, seqno, infodetcode, informationcode,
row_number() OVER (partition by accountno, currencycode order by seqno desc) as MAX_ROW
FROM (SELECT * FROM @SOURCE_EBB@.EBBS_SG_ACCINFO WHERE ods = '@ODS_EBB@' AND informationcode = 'PAY') accinfo) base2 WHERE base2.MAX_ROW = 1) t2
ON trim(account.ACCOUNTNO) = trim(t2.ACCOUNTNO) AND trim(account.CURRENCYCODE) = trim(t2.CURRENCYCODE)
LEFT JOIN
(SELECT description, isstaffclass, instclasscode FROM @SOURCE_EBB@.EBBS_SG_INSTCLS WHERE ods = '@ODS_EBB@') instcls
ON trim(mast.INSTCLASSCODE) = trim(instcls.INSTCLASSCODE)
LEFT JOIN
(SELECT description, custsegmtcode FROM @SOURCE_EBB@.EBBS_SG_CUSTSEG WHERE ods = '@ODS_EBB@') custseg
ON trim(mast.CUSTSEGMTCODE) = trim(custseg.CUSTSEGMTCODE)
LEFT JOIN
(SELECT name, segmentcode FROM @SOURCE_EBB@.EBBS_SG_SEG WHERE ods = '@ODS_EBB@') seg
ON trim(mast.SEGMENTCODE) = trim(seg.SEGMENTCODE)
LEFT JOIN
(SELECT description, isiccode FROM @SOURCE_EBB@.EBBS_SG_ISIC WHERE ods = '@ODS_EBB@') isic
ON trim(mast.ISICCODE) = trim(isic.ISICCODE)
LEFT JOIN
(SELECT description, stmttypecode FROM @SOURCE_EBB@.EBBS_SG_STMTTYP WHERE ods = '@ODS_EBB@') stmttyp
ON trim(t.STMTTYPECODE) = trim(stmttyp.STMTTYPECODE)
LEFT JOIN
(SELECT description, reasoncode FROM @SOURCE_EBB@.EBBS_SG_REASON WHERE ods = '@ODS_EBB@') reason
ON trim(account.ACCLOSUREREASON) = trim(reason.REASONCODE)
----POET Changes v1.8 ---START
LEFT JOIN (SELECT trim(DOM_DESC) AS ACCT_OPRT_INSTR_DESC,DOM_CD FROM @SOURCE_REF@.C_REF_DAT_MAP 
WHERE trim(source_country_code) = '@SOURCE_COUNTRY_CODE@' and trim(data_src) = '@DATA_SRC_EBB@' and trim(map_tp_cd) = 'AOICD') AOICD 
----POET Changes v1.8 ---END
ON trim(account.OPERATINGINS) = trim(AOICD.DOM_CD)
---v4.2---
LEFT JOIN(select * from @SOURCE_EBB@.EBBS_SG_ACCTINT where ods ='@ODS_EBB@') ACCTINT 
ON ACCTINT.CURRENCYCODE = ACCOUNT.CURRENCYCODE and ACCTINT.ACCOUNTNO = ACCOUNT.ACCOUNTNO

LEFT JOIN(SELECT * FROM (SELECT CURRENCYCODE, INTERESTCODE, EFFECTIVEDATE, craddtograteflag, draddtograteflag,ROW_NUMBER() OVER (PARTITION BY CURRENCYCODE, INTERESTCODE ORDER BY EFFECTIVEDATE DESC) AS MAX_ROW 
FROM (SELECT * FROM @SOURCE_EBB@.EBBS_SG_INTCURR WHERE EFFECTIVEDATE < '@ODS@') intcurr) BASE 
WHERE BASE.MAX_ROW=1)intc
on ACCOUNT.INTERESTCODE =INTC.INTERESTCODE and ACCOUNT.CURRENCYCODE = INTC.CURRENCYCODEc_acctmaster1001