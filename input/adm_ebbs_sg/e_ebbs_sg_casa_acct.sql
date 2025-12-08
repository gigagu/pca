;RBC~SG~C_ACCT~CASA_ACCT~14003072SGEBBselect
cacct.ACCT_SROGT_ID AS s_acct
,cacct.PRTY_SROGT_ID AS s_prty
,cacct.PROD_SROGT_ID AS s_prd
,cacct.ACCT_BRNCH_SROGT_ID AS s_brnch
,cacct.DOM_ACCT_REF AS n_acct
,cacct.DOM_PRTY_REF AS n_dom_cust
,cacct.DOM_PROD_CD AS c_dom_prd
,cacct.DOM_MAST_REF AS n_mast
,cacct.ACCT_STS_TP_CD AS c_acctsts
,cacct.ACCT_BRNCH_CD AS c_dmcle_brnch 
,cacct.ACCT_OPEN_DT AS d_acctopn
,cacct.ACCT_CLSE_DT as d_acctcls
,cprod.DOM_PROD_DESC AS x_prd
,cacct.ACCT_CURY_CD AS c_acctccy
,cabal.OS_BAL_AMT AS a_curbal_acctccy
,cacct.ACCT_TP_CD AS c_prdcat
,eacct.OBA_USD AS a_curbal_usd
,core_PID.ID_REF AS n_cust
,cbrnch.BRNCH_NM AS nm_brnch
,cacct.ACCT_STS_TP_DESC AS x_acctsts
,ecact.VLD_ACCSTS_FL AS f_vld
,cacct.SOURCE_COUNTRY_CODE AS c_cntry
,cacct.ACCT_CLSE_RESN_CD AS c_acctclsrsn
,Case when cprod.DOM_PROD_DESC LIKE '%MOA%' OR cprod.DOM_PROD_DESC LIKE '%MORTGAGE%' then 'MOA' else cprod.PROD_CAT_DESC END AS X_PRDCAT
,core_prty.SEGMT_CD AS c_glcusttyp
,coalesce(core_prty.PRTY_SEGMT_ID,'6')  AS id_glcusttyp
,Case when core_prty.SEGMT_CD='024' then 'Business Clients' else coalesce(core_prty.PRTY_SEGMT_CLAS,'UNA') end as x_glcusttyp
,cacct.REFL_ID AS ID_Reff
,cacct.SRCG_ID AS ID_Srcg
,cacct.CLSG_ID AS ID_Clsg
,eacct.OBA_LCY AS a_curbal_lcy
--,eadt.ACQ_STS_CD AS c_acqsts
,'SINGAPORE' AS nm_cntry
,cacct.ACCS_CTRY_CD  as ACCS_CTRY_CD
,cacct.ACCS_SEGMT_CD as ACCS_SEGMT_CD
,'RBC~SG~C_ACCT~CASA_ACCT~1' as PROCESS_ID
,from_unixtime(unix_timestamp()) AS PPN_DTM
,'@DT_VRSN@' AS DT_VRSN
,cacct.MNTHEND_FL AS MNTHEND_FL
,case when date_format(from_unixtime(unix_timestamp(cacct.ACCT_OPEN_DT ,'yyyyMMdd'), 'yyyy-MM-dd'),'YYYY-MM') >= date_format(from_unixtime(unix_timestamp('@PSE_DATE@' ,'yyyyMMdd'), 'yyyy-MM-dd'),'YYYY-MM') then 'NTP' else 'ETP' END AS c_acqsts

----POET Changes
,cacct.ACCT_OPRT_INSTR_CD as C_ACCT_OPRTNG_INSTR
,cacct.ACCT_OPRT_INSTR_DESC as X_ACCT_OPRTNG_INSTR
,cabal.AVAIL_BAL_AMT as A_AVLBAL_ACCTCCY
,cabal.OS_BAL_AMT_EFF_DT as D_CURBAL_EFF
,cabal.curr_stmt_cyc_os_bal_amt  AS a_curcycbal_acy
,LMT.LMT_TP_CD AS c_limit

-----POET Changes End
--April'23 Changes Start
,cprod.OD_IND AS f_od
,cacct.PROV_STS_CD AS c_provsts
,ptyltd.PRIM_PRTY_IND AS f_prim_prty
--,ecact.VLD_BS_FL AS f_vldbase
,eacct.OD_BAL AS a_odbal
,cacct.CHECKER_ID AS id_checker
,cacct.ATM_TRAN_ALWD_FL AS F_ATM_TRAN_ALWD
,cacct.CUST_ATCH_ACCT_DT AS D_CUST_ATCH_ACCT

--April'23 Changes End
--April'23 Changes Start HEERA
,ecact.CASA_VLD_BS_BAL_FL AS f_vld_base_bal
,ecact.CASA_VLD_BS_ACCT_FL AS f_vld_base_acct
--April'23 Changes End HEERA

---BOW Changes V2.7 Start
,core_prty.ENTRP_PRTY_SROGT_ID AS S_ENTRP_PRTY
,core_prty.ENTRP_PRTY_REF AS N_ENTRP_PRTY 
---BOW Changes V2.7 End

,CACT.LMT_AMT as A_LMT_AMT

--DSM Changes
,eacct.VLD_SI_FL as f_vld_si
,eacct.SI_PAYEE_FL as f_payee_setup
,ecact.LTS_PYR_DT as d_lts_pyr
,ecact.LTS_PYR_AMT as a_lts_pyr
,ecact.PRV_PYR_DT as d_prev_pyr
,ecact.PRV_PYR_AMT as a_prev_pyr
,ecact.PRV2_PYR_DT as d_prev2_pyr
,ecact.PRV2_PYR_AMT as a_prev2_pyr
,ecact.PRV3_PYR_DT as d_prev3_pyr
,ecact.PRV3_PYR_AMT as a_prev3_pyr

,eacct.OPRIST_CLS_CD as C_OPRTNG_INSTR_CLAS
,eacct.MON_ON_BOK AS N_MOB

-------DSM Changes start
,eacct.DLY_AVG_OBA_LCY_L1M  as  A_DLY_AVG_OBA_LCY_L1M
,eacct.DLY_AVG_OBA_LCY_L2M  as  A_DLY_AVG_OBA_LCY_L2M
,eacct.DLY_AVG_OBA_LCY_L3M  as  A_DLY_AVG_OBA_LCY_L3M
,eacct.DLY_AVG_OBA_LCY_L4M  as  A_DLY_AVG_OBA_LCY_L4M
,eacct.DLY_AVG_OBA_LCY_L5M  as  A_DLY_AVG_OBA_LCY_L5M
,eacct.DLY_AVG_OBA_LCY_L6M  as  A_DLY_AVG_OBA_LCY_L6M
,eacct.DLY_AVG_OBA_LCY_L7M  as  A_DLY_AVG_OBA_LCY_L7M
,eacct.DLY_AVG_OBA_LCY_L8M  as  A_DLY_AVG_OBA_LCY_L8M
,eacct.DLY_AVG_OBA_LCY_L9M  as  A_DLY_AVG_OBA_LCY_L9M
,eacct.DLY_AVG_OBA_LCY_L10M  as  A_DLY_AVG_OBA_LCY_L10M
,eacct.DLY_AVG_OBA_LCY_L11M  as  A_DLY_AVG_OBA_LCY_L11M
,eacct.DLY_AVG_OBA_LCY_L12M  as  A_DLY_AVG_OBA_LCY_L12M
-------DSM Changes end

------DSM Drop 9 changes--

,ECTARL60D.lts_ins_trn_dt_l2m as D_LTS_INS_TRAN_L2M
,ECTARL60D.lts_inv_trn_dt_l2m as D_LTS_INV_TRAN_L2M 
,ECTARL60D.lts_crd_sps_trn_dt_l2m as D_LTS_CRD_SPND_TRAN_L2M
,ECTARL60D.lts_bil_pay_trn_dt_l2m as D_LTS_BILL_PAY_TRAN_L2M
,ECTARL60D.lts_pyr_trn_dt_l2m as D_LTS_PYR_TRAN_L2M
,ECTARL60D.ins_int_fl_l2m as F_INS_INT_FL_L2M
,ECTARL60D.inv_int_fl_l2m as F_INV_INT_FL_L2M
,ECTARL60D.crd_sps_int_fl_l2m as F_CRD_SPND_INT_FL_L2M
,ECTARL60D.bil_pay_int_fl_l2m as F_BILL_PAY_INT_FL_L2M
,ECTARL60D.pyr_int_fl_l2m as F_PYR_INT_FL_L2M
,eacct.DLY_AVG_ABA_ACY_L1M AS A_DLY_AVG_AVL_BAL_ACY_L1M
--LDC5.7V STARTS--
,ETAR.LST_TRN_DT_L6M AS d_last_tran
--,eacct.VLD_SI_FL AS F_VLD_SI
,eacct.ACC_LVL_INT_FL AS F_ACCT_LVL_INT
,eacct.INT_RT_TP_CD AS C_INT_RT_TP
,eacct.ACC_CCY_CLS AS C_ACCTCCY_CLAS
,eacct.INT_PRD_CD AS C_INT_PRD
,GLCOAR.GL_BUSN_UNIT_CD AS C_GL_BUSN_UNIT
,GLCOAR.GL_ACCT_NUM AS N_GL_ACCT
,GLCOAR.GL_DEPT_CD AS C_GL_DEPT
,GLCOAR.GL_AFFL_CD AS C_GL_AFFL
,GLCOAR.GL_CUST_CLAS_CD AS C_GL_CUST_CLAS
,GLCOAR.GL_OPRT_UNIT_CD AS C_GL_OPRT_UNIT
,GLCOAR.GL_PROD_CD AS C_GL_PRD
,eadt.INT_RT_CRV AS X_INT_RT_CRV  
,cacct.PAYR_IND AS F_PYR
,intbal.LAST_CR_INT_RATE AS R_INT
--LDC v7.8 chanages
,eacct.VLD_DDR_FL AS F_VLD_DDR
--LDC 5.7V ENDS--
--LDC 6.2V STARTS--
,eacct.NUM_JNT_ACC_HLR AS K_JNT_ACCT_HLDR
--LDC 6.2V ENDS--
--LDC 6.7 STARTS--
,GLCOAR.IFRS_SCHED_CD as C_IFRS_SCHED
,GLCOAR.ACCT_SUB_LDGR_SUB_CAT as C_ASL_SUB_CAT
,GLCOAR.ACCT_SUB_LDGR_CAT as C_ASL_CAT
--LDC 6.7 changes end--
,earef.DR_INT_ACR_FL as F_DR_INT_ACR

----PDW Changes ----
,eacct.CR_TOV_AMT_LCY as  A_CR_TOV_LCY
,eacct.DR_TOV_AMT_LCY as  A_DR_TOV_LCY
,eacct.CR_TRN_CNT as K_CR_TRN
,eacct.DR_TRN_CNT as K_DR_TRN
----9.5 changes----
,intbal.CR_INT_ACR_AMT as A_CR_INTACCR

FROM 
( select  ACCT_SROGT_ID,PRTY_SROGT_ID,PROD_SROGT_ID,ACCT_BRNCH_SROGT_ID,DOM_ACCT_REF,DOM_PRTY_REF,DOM_PROD_CD,DOM_MAST_REF,ACCT_STS_TP_CD,ACCT_BRNCH_CD,ACCT_OPEN_DT,ACCT_CLSE_DT,ACCT_CURY_CD,ACCT_TP_CD,ACCT_STS_TP_DESC,SOURCE_COUNTRY_CODE,ACCT_CLSE_RESN_CD,REFL_ID,SRCG_ID,CLSG_ID,accs_ctry_cd,accs_segmt_cd,mnthend_fl

----POET Changes start
,ACCT_OPRT_INSTR_CD
,ACCT_OPRT_INSTR_DESC
----POET Changes end
--April'23 Changes Start
,PROV_STS_CD
,CHECKER_ID
,ATM_TRAN_ALWD_FL
,CUST_ATCH_ACCT_DT
,PAYR_IND
--April'23 Changes End
from @TARGET_WORK@.c_acct where process_date = '@PSE_DATE@' AND DATA_SRC = '@DATA_SRC_EBB@' AND  SOURCE_COUNTRY_CODE ='@SOURCE_COUNTRY_CODE@' and process_id='EBB~SG~EBBS_SG_ACCOUNT~C_ACCT~1') cacct

left join (select PRTY_SROGT_ID, ID_REF from @TARGET_WORK@.C_PRTY_ID  where  process_date = '@PSE_DATE@' AND DATA_SRC = '@DATA_SRC_EBB@' AND  SOURCE_COUNTRY_CODE ='@SOURCE_COUNTRY_CODE@' and ID_TP_CD = 'N01')core_PID on trim(cacct.prty_srogt_id) = trim(core_PID.PRTY_SROGT_ID) and core_PID.prty_srogt_id <> ''

left join (select prty_srogt_id, SEGMT_CD,PRTY_SEGMT_ID,PRTY_SEGMT_CLAS,ENTRP_PRTY_SROGT_ID,ENTRP_PRTY_REF from @TARGET_WORK@.c_prty where process_date = '@PSE_DATE@' AND DATA_SRC = '@DATA_SRC_EBB@' AND  SOURCE_COUNTRY_CODE ='@SOURCE_COUNTRY_CODE@')core_prty on trim(cacct.prty_srogt_id) = trim(core_prty.prty_srogt_id) and core_prty.prty_srogt_id <> ''

left join (select OS_BAL_AMT,acct_srogt_id

----POET Changes start 
,AVAIL_BAL_AMT
,OS_BAL_AMT_EFF_DT
,curr_stmt_cyc_os_bal_amt
----POET Changes End
from  @TARGET_WORK@.C_ACCT_BAL_MTRC where process_date = '@PSE_DATE@' AND data_src = '@DATA_SRC_EBB@' and source_country_code ='@SOURCE_COUNTRY_CODE@') cabal on cabal.acct_srogt_id = cacct.acct_srogt_id and cabal.acct_srogt_id <>''

left join (select brnch_srogt_id,BRNCH_NM from @TARGET_WORK@.C_BRNCH where process_date = '@PSE_DATE@' AND data_src = '@DATA_SRC_EBB@' and source_country_code ='@SOURCE_COUNTRY_CODE@') cbrnch on cbrnch.brnch_srogt_id = cacct.acct_brnch_srogt_id and cbrnch.brnch_srogt_id <>''

left join (select acct_srogt_id,VLD_ACCSTS_FL
--April'23 Changes Start HEERA
,CASA_VLD_BS_BAL_FL
,CASA_VLD_BS_ACCT_FL
--April'23 Changes End HEERA
,LTS_PYR_DT
,LTS_PYR_AMT
,PRV_PYR_DT
,PRV_PYR_AMT
,PRV2_PYR_DT
,PRV2_PYR_AMT
,PRV3_PYR_DT
,PRV3_PYR_AMT
 from  @TARGET_WORK@.E_CORE_ACCT_BASE where process_date = '@PSE_DATE@' AND data_src = '@DATA_SRC_EBB@' and source_country_code ='@SOURCE_COUNTRY_CODE@' and process_id='EBB~SG~EBBS_SG_ACCOUNT~E_CORE_ACCT_BASE~1') ecact on ecact.acct_srogt_id = cacct.acct_srogt_id and ecact.acct_srogt_id <>''
 
 left join (select acct_srogt_id,prty_srogt_id,PRIM_PRTY_IND
 from  @TARGET_WORK@.C_PRTY_ACCT_RLTD where process_date = '@PSE_DATE@' AND data_src = '@DATA_SRC_EBB@' and source_country_code ='@SOURCE_COUNTRY_CODE@' and process_id='EBB~SG~EBBS_SG_ACCOUNT~C_PRTY_ACCT_RLTD~1') ptyltd on ptyltd.acct_srogt_id = cacct.acct_srogt_id and trim(ptyltd.prty_srogt_id) = trim(cacct.prty_srogt_id)

--left join (select acct_srogt_id,ACQ_STS_CD from  @TARGET_WORK@.E_CORE_ACCT_DTL where process_date = '@PSE_DATE@' AND data_src = '@DATA_SRC_EBB@' and source_country_code ='@SOURCE_COUNTRY_CODE@') eadt on eadt.acct_srogt_id = cacct.acct_srogt_id and eadt.acct_srogt_id <>''

left join (select distinct acct_srogt_id,int_rt_crv from @TARGET_WORK@.E_ACCT_DTL where process_date = '@PSE_DATE@' AND data_src = '@DATA_SRC_EBB@' and source_country_code ='@SOURCE_COUNTRY_CODE@') eadt on eadt.acct_srogt_id = cacct.acct_srogt_id and eadt.acct_srogt_id <>''

left join (select acct_srogt_id,OBA_USD,OBA_LCY,OD_BAL,VLD_SI_FL,SI_PAYEE_FL,OPRIST_CLS_CD,MON_ON_BOK,VLD_SI_FL,ACC_CCY_CLS,ACC_LVL_INT_FL,INT_RT_TP_CD,NUM_JNT_ACC_HLR,INT_PRD_CD,VLD_DDR_FL
-------DSM changes start
,DLY_AVG_OBA_LCY_L1M
,DLY_AVG_OBA_LCY_L2M
,DLY_AVG_OBA_LCY_L3M
,DLY_AVG_OBA_LCY_L4M
,DLY_AVG_OBA_LCY_L5M
,DLY_AVG_OBA_LCY_L6M
,DLY_AVG_OBA_LCY_L7M
,DLY_AVG_OBA_LCY_L8M
,DLY_AVG_OBA_LCY_L9M
,DLY_AVG_OBA_LCY_L10M
,DLY_AVG_OBA_LCY_L11M
,DLY_AVG_OBA_LCY_L12M
-------DSM changes end
,DLY_AVG_ABA_ACY_L1M
,CR_TOV_AMT_LCY
,DR_TOV_AMT_LCY
,CR_TRN_CNT
,DR_TRN_CNT

from @TARGET_WORK@.E_ACCT_BASE where process_date = '@PSE_DATE@' AND data_src = '@DATA_SRC_EBB@' and source_country_code ='@SOURCE_COUNTRY_CODE@' and process_id='EBB~SG~EBBS_SG_ACCOUNT~E_ACCT_BASE~1') eacct on eacct.acct_srogt_id = cacct.acct_srogt_id and eacct.acct_srogt_id <>''
left join (select prod_srogt_id,PROD_CAT_DESC,DOM_PROD_DESC
--April'23 Changes End
,OD_IND
--April'23 Changes End
from @TARGET_WORK@.C_PROD where process_date = '@PSE_DATE@' AND data_src = '@DATA_SRC_EBB@' and source_country_code ='@SOURCE_COUNTRY_CODE@') cprod on cprod.prod_srogt_id = cacct.prod_srogt_id and cprod.prod_srogt_id <> ''
LEFT JOIN (SELECT * FROM @TARGET_WORK@.C_APRVD_LMT where process_date = '@PSE_DATE@' AND data_src = '@DATA_SRC_EBB@' and source_country_code ='@SOURCE_COUNTRY_CODE@' and  PROCESS_ID = 'EBB~SG~EBBS_SG_MLTMAST~C_APRVD_LMT~1' ) LMT
ON CACCT.acct_srogt_id = LMT.LMT_SROGT_ID AND LMT.LMT_SROGT_ID <> ''
LEFT JOIN (SELECT * FROM @TARGET_WORK@.C_ACTVD_LMT where process_date = '@PSE_DATE@' AND data_src = '@DATA_SRC_EBB@' and source_country_code ='@SOURCE_COUNTRY_CODE@' and PROCESS_ID = 'EBB~SG~EBBS_SG_ACCTLMT~C_ACTVD_LMT~1' ) CACT
ON CACCT.acct_srogt_id = CACT.ACTVD_LMT_SROGT_ID
LEFT JOIN(SELECT acct_srogt_id,lts_ins_trn_dt_l2m,lts_inv_trn_dt_l2m,lts_crd_sps_trn_dt_l2m,lts_bil_pay_trn_dt_l2m,lts_pyr_trn_dt_l2m,ins_int_fl_l2m,inv_int_fl_l2m,crd_sps_int_fl_l2m,bil_pay_int_fl_l2m,pyr_int_fl_l2m
FROM @TARGET_WORK@.E_CORE_TRAN_ACCT_ROLLUP_L60D where process_date = '@PSE_DATE@' AND data_src = '@DATA_SRC_EBB@' and source_country_code = '@SOURCE_COUNTRY_CODE@') ECTARL60D 
on cacct.acct_srogt_id = ECTARL60D.acct_srogt_id
--LDC5.7V STARTS--
LEFT JOIN(SELECT distinct acct_srogt_id,LST_TRN_DT_L6M FROM @TARGET_WORK@.E_CORE_TRAN_ACCT_ROLLUP_L180D where process_date = '@PSE_DATE@' AND  data_src ='@DATA_SRC_EBB@' and source_country_code ='@SOURCE_COUNTRY_CODE@')ETAR on cacct.acct_srogt_id = ETAR.acct_srogt_id

LEFT JOIN (Select * from (select * from (Select *, row_number() over (partition by ACCT_SROGT_ID order by ACCT_SROGT_ID, GL_CUST_CLAS_CD) as n from @TARGET_WORK@.C_ACCT_GL_COA_RLTD where process_date = '@PSE_DATE@' AND DATA_SRC = 'RAH' and SOURCE_COUNTRY_CODE = 'XX' AND TRIM(IFRS_SCHED_CD) = '2V' and TRIM(SRC_SYS_CD) = 'EBBS' AND TRIM(ACCS_CTRY_CD) = 'SG')k where k.n=1)b)GLCOAR
on cacct.acct_srogt_id = GLCOAR.acct_srogt_id

left join (select distinct LAST_CR_INT_RATE,acct_srogt_id,CR_INT_ACR_AMT from @TARGET_WORK@.C_ACCT_INT_MTRC where process_date = '@PSE_DATE@' AND data_src = '@DATA_SRC_EBB@' and source_country_code ='@SOURCE_COUNTRY_CODE@') intbal
on intbal.acct_srogt_id = cacct.acct_srogt_id and intbal.acct_srogt_id <> ''

LEFT JOIN(select * from  @TARGET_WORK@.C_ACCT_REF where process_date = '@PSE_DATE@' and data_src = '@DATA_SRC_EBB@' and source_country_code ='@SOURCE_COUNTRY_CODE@') earef on earef.acct_srogt_id = cacct.acct_srogt_id and earef.acct_srogt_id <> ''casa_acctmaster1001