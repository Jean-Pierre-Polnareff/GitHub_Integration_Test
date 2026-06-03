

-- ================================= ============
-- Author:		Vladislav Pilipets
-- Create date: 2022-05-09
-- Description:	Compliance Items Reporting
-- =============================================
CREATE PROCEDURE [dbo].[usp_12_SQL_FLD_Report_Update]
	@mail_profile		varchar(50), 
	@mail_to			varchar(1500), 
	@mail_cc			varchar(1500), 
	@exec_db			varchar(50), 
	@exec_date			datetime = null
AS  
BEGIN

SET NOCOUNT ON;

DECLARE @prefix_subject AS VARCHAR(50) = (SELECT attributes FROM Speech_Analytics.dbo.cm_connattr WHERE parameter = 'prefix' AND isActive = 1) 

--Setting a current for automatic run
DECLARE @Exe_Date DATETIME
IF @exec_date IS NULL 
BEGIN 
	SET @Exe_Date= GETDATE()-3 
END 
ELSE 
BEGIN 
	SET @Exe_Date= @exec_date-3
END 
PRINT @Exe_Date


--Setting a eomdate for automatic run
DECLARE @End_Month DATETIME
SET @End_Month=EOMONTH(CAST(DATEADD(DAY,0,@Exe_Date) AS DATE ),0)
--SET @End_Month=EOMONTH(CAST(DATEADD(DAY,-3,GETDATE()) AS DATE ),0)
PRINT @End_Month

--Setting a 48hr  for automatic run
DECLARE @Call_Month DATETIME
SET @Call_Month=CAST(@Exe_Date-2 AS DATE)
PRINT @Call_Month


IF OBJECT_ID('tempdb.dbo.#Results') IS NOT NULL 
DROP TABLE dbo.#Results
SELECT DISTINCT a.EOM,a.AGNTID,a.CALLID,a.CALLDT,a.RECDP,
a.RGSACC,a.DIS1,a.SKNM,a.CLDUR,a.RGSSEID,a.PHNUMB,a.DIR,a.CLNTID,
b.DIS1CAT,case when b.DIS1CAT='RPC' and 
a.CLDUR between 180 and 250 then 'DNERPC250'
when b.DIS1CAT='RPC' and a.CLDUR between 251 and 400 THEN 'DNERPC400'
when b.DIS1CAT='RPC' and a.CLDUR>=600 then 'DNERPC600' WHEN 
b.DIS1CAT='PTP' then 'DNEPTP'
when b.DIS1CAT='TPC' then 'DNETHRDP'ELSE 'DNELM' END AS CallCDe
INTO dbo.#Results FROM dbo.CM_CALLEXPORT a LEFT JOIN
dbo.CM_LVCDE b on a.DIS1=b.DIS1 and a.RECDP=b.RECDP
WHERE a.EOM=@End_Month AND a.SMPLFLG=1

-- Exracting Callid except No category hits
IF OBJECT_ID('tempdb.dbo.#Results1') IS NOT NULL 
DROP TABLE dbo.#Results1
SELECT EOM,CALLDT,CALLID,CATHIT,COMPNAME,STTIME,ENDTIME 
INTO dbo.#Results1 FROM dbo.CM_EVNTSAPICMP
WHERE CALLID IN (SELECT CALLID FROM dbo.#Results)
AND CATHIT<>'No_Category'

--counting the number of category hits
IF OBJECT_ID('tempdb.dbo.#Results1_1') IS NOT NULL 
DROP TABLE dbo.#Results1_1
SELECT CALLID,COUNT(CATHIT) AS EVNTSCNT 
INTO dbo.#Results1_1 FROM dbo.#Results1
GROUP BY CALLID

--extracting Consumer language to gauge the opening opprotunities
IF OBJECT_ID('tempdb.dbo.#Results1_2') IS NOT NULL 
DROP TABLE dbo.#Results1_2
SELECT DISTINCT CALLID,CATHIT,STTIME 
INTO dbo.#Results1_2 
FROM dbo.#Results1
WHERE CATHIT IN ('EN Avoider Consumer','EN Consumer Misunderstanding',
'EN Controller Consumer','EN COVID19',
'EN Employment Pitch','EN Income Pitch','EN Pitcher Consumer',
'EN Promiser Consumer','EN Attorney CS','EN AXP OOS',
'EN Balance Dispute','EN Bankruptcy',
'EN Fraud Dispute','EN General Dispute','EN SCRA')

---first category hit Consumer language to gauge the opening opprotunities
IF OBJECT_ID('tempdb.dbo.#Results1_3') IS NOT NULL 
DROP TABLE dbo.#Results1_3
SELECT a.CALLID,a.STTIME
INTO dbo.#Results1_3
FROM(SELECT CALLID,CATHIT,STTIME,
ROW_NUMBER() OVER (PARTITION BY CALLID ORDER BY STTIME DESC) AS rwnumb 
FROM dbo.#Results1_2) a
WHERE a.rwnumb=1

--extracting calls where Voiclemail language was hit
IF OBJECT_ID('tempdb.dbo.#Results1_2_1') IS NOT NULL 
DROP TABLE dbo.#Results1_2_1
SELECT DISTINCT CALLID 
INTO dbo.#Results1_2_1 
FROM dbo.#Results1
WHERE CATHIT='EN Message Machine'

--extracting calls where Spanish language was hit
IF OBJECT_ID('tempdb.dbo.#Results1_2_2') IS NOT NULL 
DROP TABLE dbo.#Results1_2_2
SELECT DISTINCT CALLID 
INTO dbo.#Results1_2_2 
FROM dbo.#Results1
WHERE CATHIT='EN Possible Spanish CS'

--removing calls where I had no category hit
IF OBJECT_ID('tempdb.dbo.#Results_1') IS NOT NULL 
DROP TABLE dbo.#Results_1
SELECT a.EOM,a.AGNTID,a.CALLID,a.CALLDT,a.RECDP,a.RGSACC,a.DIS1,a.SKNM,
a.CLDUR,a.RGSSEID,a.PHNUMB,a.DIS1CAT,a.CallCDe,a.DIR,a.CLNTID
INTO dbo.#Results_1 
FROM dbo.#Results a LEFT JOIN 
dbo.#Results1_1 b 
ON b.CALLID = a.CALLID
WHERE b.EVNTSCNT>0 AND a.DIR='Outbound'

--taking calls only categories as RPC & PTP
IF OBJECT_ID('tempdb.dbo.#Results_2') IS NOT NULL 
DROP TABLE dbo.#Results_2
SELECT A.EOM,A.AGNTID,A.CALLID,A.CALLDT,A.RECDP,A.RGSACC,A.DIS1,A.SKNM,
A.CLDUR,A.RGSSEID,A.PHNUMB,A.DIS1CAT,A.CALLCDE,A.DIR,a.CLNTID
INTO dbo.#Results_2
FROM dbo.#Results_1 a
WHERE a.DIS1CAT IN ('RPC','PTP')


--MRD Calls
IF OBJECT_ID('tempdb.dbo.#Results1_4') IS NOT NULL 
DROP TABLE dbo.#Results1_4
SELECT DISTINCT CALLID,CATHIT 
INTO dbo.#Results1_4 
FROM dbo.#Results1
WHERE CATHIT='EN MRD'

--MM Calls
IF OBJECT_ID('tempdb.dbo.#Results1_5') IS NOT NULL 
DROP TABLE dbo.#Results1_5
SELECT DISTINCT CALLID,CATHIT 
INTO dbo.#Results1_5 
FROM dbo.#Results1
WHERE CATHIT='EN AXP Mini Miranda'

--Creditor
IF OBJECT_ID('tempdb.dbo.#Results1_6') IS NOT NULL 
DROP TABLE dbo.#Results1_6
SELECT DISTINCT CALLID,CATHIT 
INTO dbo.#Results1_6 
FROM dbo.#Results1
WHERE CATHIT='EN AXP Creditor Name'


--Balance
IF OBJECT_ID('tempdb.dbo.#Results1_7') IS NOT NULL 
DROP TABLE dbo.#Results1_7
SELECT DISTINCT CALLID,CATHIT,STTIME
INTO dbo.#Results1_7
FROM (SELECT CALLID,CATHIT,STTIME,
ROW_NUMBER() OVER (PARTITION BY CALLID ORDER BY STTIME) AS rwnumb 
FROM dbo.#Results1
WHERE CATHIT='EN AXP Balance')a
WHERE rwnumb=1

--combing all opening parameter hits
IF OBJECT_ID('tempdb.dbo.#Results_3') IS NOT NULL 
DROP TABLE dbo.#Results_3
SELECT A.EOM,A.AGNTID,A.CALLID,A.CALLDT,A.RECDP,A.RGSACC,A.DIS1,A.SKNM,
A.CLDUR,A.RGSSEID,A.PHNUMB,A.DIS1CAT,A.CALLCDE,A.DIR,A.CLNTID,1 AS CALLCNT,
CASE WHEN B.CALLID IS NOT NULL THEN 1 ELSE 0 END AS MRDHIT,
CASE WHEN C.CALLID IS NOT NULL THEN 1 ELSE 0 END AS MMHIT,
CASE WHEN D.CALLID IS NOT NULL THEN 1 ELSE 0 END AS CRDHIT,
CASE WHEN E.CALLID IS NOT NULL  THEN 1 ELSE 0 END AS BALHIT,E.STTIME
INTO dbo.#Results_3
FROM DBO.#Results_2 A 
LEFT JOIN DBO.#Results1_4 B ON A.CALLID=B.CALLID
LEFT JOIN DBO.#Results1_5 c ON A.CALLID=c.CALLID
LEFT JOIN DBO.#Results1_6 d ON A.CALLID=d.CALLID
LEFT JOIN DBO.#Results1_7 e ON A.CALLID=e.CALLID
ORDER BY a.CALLDT

--balance & creditor hit logic applied
IF OBJECT_ID('tempdb.dbo.#Results_4') IS NOT NULL 
DROP TABLE dbo.#Results_4
SELECT A.EOM,A.AGNTID,A.CALLID,A.CALLDT,A.RECDP,A.RGSACC,A.DIS1,A.SKNM,
A.CLDUR,A.RGSSEID,A.PHNUMB,A.DIS1CAT,A.CALLCDE,A.DIR,a.CLNTID,
a.CALLCNT, a.MRDHIT AS MRDHIT1,
a.MMHIT AS MMHIT1,CASE WHEN a.BALHIT=1 AND a.CRDHIT=0 
THEN 1 ELSE CRDHIT END AS CRDHIT1,
a.BALHIT,a.STTIME
INTO dbo.#Results_4
FROM dbo.#Results_3 a
ORDER BY a.CALLDT

--mm and MRD hit logic applied
IF OBJECT_ID('tempdb.dbo.#Results_5') IS NOT NULL 
DROP TABLE dbo.#Results_5
SELECT a.EOM,a.AGNTID,a.CALLID,a.CALLDT,a.RECDP,a.RGSACC,a.DIS1,a.SKNM,a.CLDUR,
a.RGSSEID,a.PHNUMB,a.DIS1CAT,a.CallCDe,a.DIR,A.CLNTID,a.CALLCNT,
CASE WHEN a.MMHIT1=1 AND a.MRDHIT1=0 THEN 1 ELSE a.MRDHIT1 END AS MRDHIT3,
a.MMHIT1,a.CRDHIT1,a.BALHIT,a.STTIME 
INTO dbo.#Results_5
FROM dbo.#Results_4 a

-- Bal oppotunirty logic
IF OBJECT_ID('tempdb.dbo.#Results_6') IS NOT NULL 
DROP TABLE dbo.#Results_6
SELECT a.EOM,a.AGNTID,a.CALLID,a.CALLDT,a.RECDP,
a.RGSACC,a.DIS1,a.SKNM,a.CLDUR,a.RGSSEID,
a.PHNUMB,a.DIS1CAT,a.CallCDe,a.DIR,A.CLNTID,a.CALLCNT,a.MRDHIT3,
a.MMHIT1,a.CRDHIT1,a.BALHIT,a.STTIME,b.STTIME as cxbst,
CASE WHEN ISNULL(a.STTIME,0)<ISNULL(b.STTIME,0) OR 
a.BALHIT=1 THEN 1 ELSE 0 END AS BALOPP
INTO dbo.#Results_6
FROM dbo.#Results_5 a LEFT JOIN dbo.#Results1_3 b 
ON b.CALLID = a.CALLID

--RPC Table for combining the results
IF OBJECT_ID('tempdb.dbo.#Results_7') IS NOT NULL 
DROP TABLE dbo.#Results_7
SELECT a.EOM,a.AGNTID,a.CALLID,a.CALLDT,a.RECDP,
a.RGSACC,a.DIS1,a.SKNM,a.CLDUR,a.RGSSEID,
a.PHNUMB,a.DIS1CAT,a.CallCDe,a.DIR,a.CLNTID,a.CALLCNT,
a.MRDHIT3,a.MMHIT1,a.CRDHIT1,a.BALHIT,a.STTIME,
1 AS MRDOPP,1 as MMOPP,1 AS CRDOPP,BALOPP,0 AS INCTR
INTO dbo.#Results_7
FROM dbo.#Results_6 a

--extracting 3rd Party Calls
IF OBJECT_ID('tempdb.dbo.#Results_8') IS NOT NULL 
DROP TABLE dbo.#Results_8
SELECT A.EOM,A.AGNTID,A.CALLID,A.CALLDT,A.RECDP,A.RGSACC,A.DIS1,A.SKNM,
A.CLDUR,A.RGSSEID,A.PHNUMB,A.DIS1CAT,A.CALLCDE,A.DIR,A.CLNTID
INTO dbo.#Results_8
FROM dbo.#Results_1 a
WHERE a.DIS1CAT ='TPC'

-- checking for only MRD on 3rd party
IF OBJECT_ID('tempdb.dbo.#Results_9') IS NOT NULL 
DROP TABLE dbo.#Results_9
SELECT A.EOM,A.AGNTID,A.CALLID,A.CALLDT,A.RECDP,A.RGSACC,A.DIS1,A.SKNM,
A.CLDUR,A.RGSSEID,A.PHNUMB,A.DIS1CAT,A.CALLCDE,A.DIR,a.CLNTID,1 AS CALLCNT,
CASE WHEN B.CALLID IS NOT NULL THEN 1 ELSE 0 END AS MRDHIT,
0 AS MMHIT,0 AS CRDHIT,0 AS BALHIT,E.STTIME
INTO dbo.#Results_9
FROM DBO.#Results_8 A 
LEFT JOIN DBO.#Results1_4 B ON A.CALLID=B.CALLID
LEFT JOIN DBO.#Results1_5 c ON A.CALLID=c.CALLID
LEFT JOIN DBO.#Results1_6 d ON A.CALLID=d.CALLID
LEFT JOIN DBO.#Results1_7 e ON A.CALLID=e.CALLID
ORDER BY a.CALLDT

-- 3rd party opportunities and incorrect termination
IF OBJECT_ID('tempdb.dbo.#Results_10') IS NOT NULL 
DROP TABLE dbo.#Results_10
SELECT a.EOM,a.AGNTID,a.CALLID,a.CALLDT,a.RECDP,a.RGSACC,a.DIS1,
a.SKNM,a.CLDUR,a.RGSSEID,a.PHNUMB,a.DIS1CAT,a.CallCDe,a.DIR,a.CLNTID,
a.CALLCNT,a.MRDHIT AS MRDHIT3,a.MMHIT AS MMHIT1,a.CRDHIT AS CRDHIT1,
a.BALHIT,a.STTIME,1 AS MRDOPP,0 as MMOPP,0 AS CRDOPP,0 AS BALOPP,
CASE WHEN ISNULL(a.STTIME,0)<b.STTIME THEN 1 ELSE 0 END AS INCTR
INTO dbo.#Results_10
FROM dbo.#Results_9 a LEFT JOIN dbo.#Results1_3 b ON b.CALLID = a.CALLID

--combining RPC and TPC
IF OBJECT_ID('tempdb.dbo.#Results_11') IS NOT NULL 
DROP TABLE dbo.#Results_11
SELECT EOM,AGNTID,CALLID,CALLDT,RECDP,RGSACC,DIS1,SKNM,CLDUR,RGSSEID,
PHNUMB,DIS1CAT,CallCDe,DIR,CLNTID,CALLCNT,MRDHIT3,MMHIT1,CRDHIT1,
BALHIT,STTIME,MRDOPP,MMOPP,CRDOPP,BALOPP,INCTR
INTO dbo.#Results_11
FROM 
(SELECT EOM,AGNTID,CALLID,CALLDT,RECDP,RGSACC,DIS1,SKNM,CLDUR,RGSSEID,
PHNUMB,DIS1CAT,CallCDe,DIR,CLNTID,CALLCNT,MRDHIT3,MMHIT1,CRDHIT1,
BALHIT,STTIME,MRDOPP,MMOPP,CRDOPP,BALOPP,INCTR FROM dbo.#Results_7
UNION
SELECT EOM,AGNTID,CALLID,CALLDT,RECDP,RGSACC,DIS1,SKNM,CLDUR,RGSSEID,
PHNUMB,DIS1CAT,CallCDe,DIR,CLNTID,CALLCNT,MRDHIT3,MMHIT1,CRDHIT1,
BALHIT,STTIME,MRDOPP,MMOPP,CRDOPP,BALOPP,INCTR FROM dbo.#Results_10) a

--MM opportunity and hit calculation based of client ids
IF OBJECT_ID('tempdb.dbo.#Results_14') IS NOT NULL 
DROP TABLE dbo.#Results_14
SELECT a.EOM,a.AGNTID,a.CALLID,a.CALLDT,a.RECDP,a.RGSACC,a.DIS1,a.SKNM,a.CLDUR,a.RGSSEID,
a.PHNUMB,a.DIS1CAT,a.CallCDe,a.DIR,a.CALLCNT,a.MRDHIT3,
case when a.RECDP='Veldos' and a.CLNTID not in ('111BHIR','111BMDR','111CDDR','111CHBR','111CHER','111CLBR','111GURR',
'111HEPR','111LEPR','111LHIR','111LLOR','111LMDR','111LSPR','111OHIR','111OLOR','111OMDR','111REPR',
'111WAIR','112AEPR','112ALLR','112ASPR','112LOWR','112MEDR','113ASPR','113HIGR','114QCCR','11MCLHR',
'11MCLLR','11MCOHR','11MCOLR','11MLHIR','11MLLOR','11MLMDR','11MOHIR','11MOLOR','11MOMDR','R14SM','R15ZM',
'R1F7M','R1FQM','R1FVM','R1G7M','R1GQM','R1J2M','R1J8M','R1J9M','R1JQM','R1JRM','R1JWM','R1JZM',
'R1K4M','R1K6M','R1K8M','R1KEM','R1KQM','R1KSM','R1KWM','R1KZM','R1L8M','R1L9M','R1LQM','R1LRM',
'R1MQM','R1QZM','R1RWM','R1RZM','R1W1M','R1W2M','R1W3M','R1W5M','R1W7M','R1X1M','R1X2M','R1X3M',
'R1X5M','R1X7M','R201M','R24WM','R24ZM','R25ZM','R2GBM','R2HDM','R34ZM','R3CFM','R3JBM','R3PGR',
'R3POR','R3PSR','R3PTR','RCGAR','RGN0M','RLK1M','RLK2M','RLK3M','RLK4M','RLT1M','RLUJM','12LJ2AR',
'12LJ2JR','12LJ2ZR','113CFRR','113LFRR','113OFRR','113SFRR','112HIGR','113AEPR','113HI3R','113HI1R',
'113HI2R','14SM','15ZM','1F7M','1FQM','1FVM','1G7M','1GQM','1J2M','1J8M','1J9M','1JQM','1JRM','1JWM',
'1JZM','1K4M','1K6M','1K8M','1KEM','1KQM','1KSM','1KWM','1KZM','1L8M','1L9M','1LQM','1LRM','1MQM','1QZM',
'1RWM','1RZM','1W1M','1W2M','1W3M','1W5M','1W7M','1X1M','1X2M','1X3M','1X5M','1X7M','201M','24WM','24ZM',
'25ZM','2GBM','2HDM','34ZM','3CFM','3JBM','3PGR','3POR','3PSR','3PTR','CGAR','GN0M','LK1M','LK2M','LK3M',
'LK4M','LT1M','LUJM') then 0 else a.MMHIT1 end as MMHIT1 ,a.CRDHIT1,
a.BALHIT,a.STTIME,a.MRDOPP,
case when a.RECDP='Veldos' and a.CLNTID not in ('111BHIR','111BMDR','111CDDR','111CHBR','111CHER','111CLBR','111GURR',
'111HEPR','111LEPR','111LHIR','111LLOR','111LMDR','111LSPR','111OHIR','111OLOR','111OMDR','111REPR',
'111WAIR','112AEPR','112ALLR','112ASPR','112LOWR','112MEDR','113ASPR','113HIGR','114QCCR','11MCLHR',
'11MCLLR','11MCOHR','11MCOLR','11MLHIR','11MLLOR','11MLMDR','11MOHIR','11MOLOR','11MOMDR','R14SM','R15ZM',
'R1F7M','R1FQM','R1FVM','R1G7M','R1GQM','R1J2M','R1J8M','R1J9M','R1JQM','R1JRM','R1JWM','R1JZM',
'R1K4M','R1K6M','R1K8M','R1KEM','R1KQM','R1KSM','R1KWM','R1KZM','R1L8M','R1L9M','R1LQM','R1LRM',
'R1MQM','R1QZM','R1RWM','R1RZM','R1W1M','R1W2M','R1W3M','R1W5M','R1W7M','R1X1M','R1X2M','R1X3M',
'R1X5M','R1X7M','R201M','R24WM','R24ZM','R25ZM','R2GBM','R2HDM','R34ZM','R3CFM','R3JBM','R3PGR',
'R3POR','R3PSR','R3PTR','RCGAR','RGN0M','RLK1M','RLK2M','RLK3M','RLK4M','RLT1M','RLUJM','12LJ2AR',
'12LJ2JR','12LJ2ZR','113CFRR','113LFRR','113OFRR','113SFRR','112HIGR','113AEPR','113HI3R','113HI1R',
'113HI2R','14SM','15ZM','1F7M','1FQM','1FVM','1G7M','1GQM','1J2M','1J8M','1J9M','1JQM','1JRM','1JWM',
'1JZM','1K4M','1K6M','1K8M','1KEM','1KQM','1KSM','1KWM','1KZM','1L8M','1L9M','1LQM','1LRM','1MQM','1QZM',
'1RWM','1RZM','1W1M','1W2M','1W3M','1W5M','1W7M','1X1M','1X2M','1X3M','1X5M','1X7M','201M','24WM','24ZM',
'25ZM','2GBM','2HDM','34ZM','3CFM','3JBM','3PGR','3POR','3PSR','3PTR','CGAR','GN0M','LK1M','LK2M','LK3M',
'LK4M','LT1M','LUJM') then 0 else a.MMOPP end as MMOPP,a.CRDOPP,a.BALOPP,a.INCTR,a.CLNTID
INTO dbo.#Results_14
FROM dbo.#Results_11 a

--changes recieved by qa or compliance based of call listening on disposition code
IF OBJECT_ID('tempdb.dbo.#Results_15') IS NOT NULL 
DROP TABLE dbo.#Results_15
select  EOM,AGNTID,CALLID,CALLDT,RECDP,RGSACC,DIS1,SKNM,CLDUR,RGSSEID,
PHNUMB,DIS1CAT,CallCDe,DIR,CALLCNT,MRDHIT3,
case when DIS1='AGENT - CUST RPC 4' and RECDP in ('RGS_CCS','Northland_Group') then 0 else MMHIT1 end as MMHIT1 ,
case when DIS1='AGENT - CUST RPC 4' and RECDP in ('RGS_CCS','Northland_Group') then 0 else CRDHIT1 end as CRDHIT1,
case when DIS1='AGENT - CUST RPC 4' and RECDP in ('RGS_CCS','Northland_Group') then 0 else BALHIT end as BALHIT,
case when DIS1='AGENT - CUST RPC 4' and RECDP in ('RGS_CCS','Northland_Group') then 0 else STTIME end as STTIME,
MRDOPP,
case when DIS1='AGENT - CUST RPC 4' and RECDP in ('RGS_CCS','Northland_Group') then 0 else MMOPP end as MMOPP,
case when DIS1='AGENT - CUST RPC 4' and RECDP in ('RGS_CCS','Northland_Group') then 0 else CRDOPP end as CRDOPP,
case when DIS1='AGENT - CUST RPC 4' and RECDP in ('RGS_CCS','Northland_Group') then 0 else BALOPP end as BALOPP,
INCTR,CLNTID 
INTO dbo.#Results_15 
from dbo.#Results_14

--changes recieved by qa or compliance based of call listening on Skills
IF OBJECT_ID('tempdb.dbo.#Results_16') IS NOT NULL 
DROP TABLE dbo.#Results_16
select  EOM,AGNTID,CALLID,CALLDT,RECDP,RGSACC,DIS1,SKNM,CLDUR,RGSSEID,
PHNUMB,DIS1CAT,CallCDe,DIR,CALLCNT,MRDHIT3,MMHIT1,CRDHIT1,BALHIT,STTIME,
MRDOPP,
case when SKNM in ('PAN_3P-ALLY_TA_Manual','OP_TA-LC TERT HCI','DEC_3P-Penncro TA_HCI','DEC_3P-Quest Diagnostics TA HCI',
'DEC_3P-Cox TA HCI','OP_TA_Bertlesmann HCI','Cap One Card TA HCI 382','RGS_JAX_LabCorp_TA_HCI',
'Cap One Card TA HCI 380','CavalryMain Preview_TA 347','RGS_JAX_JeffCap_TA_HCI','RGS_JAX_Ally_TA_HCI',
'DEC_3P-CLink TA HCI 1','DEC_3P-Ally_TA_HCI','DEC_3P-JFC TA HCI','CavalryMain HCI_TA 347',
'OP_TA-SMC HCI','RGS_JAX_Bureaus TA_HCI','DEC_3P-CLink TA HCI 2') and MMHIT1=0 then 0 else MMOPP end as MMOPP,
case when SKNM in ('PAN_3P-ALLY_TA_Manual','OP_TA-LC TERT HCI','DEC_3P-Penncro TA_HCI','DEC_3P-Quest Diagnostics TA HCI',
'DEC_3P-Cox TA HCI','OP_TA_Bertlesmann HCI','Cap One Card TA HCI 382','RGS_JAX_LabCorp_TA_HCI',
'Cap One Card TA HCI 380','CavalryMain Preview_TA 347','RGS_JAX_JeffCap_TA_HCI','RGS_JAX_Ally_TA_HCI',
'DEC_3P-CLink TA HCI 1','DEC_3P-Ally_TA_HCI','DEC_3P-JFC TA HCI','CavalryMain HCI_TA 347',
'OP_TA-SMC HCI','RGS_JAX_Bureaus TA_HCI','DEC_3P-CLink TA HCI 2') and CRDHIT1=0 then 0 else CRDOPP end as CRDOPP,
case when SKNM in ('PAN_3P-ALLY_TA_Manual','OP_TA-LC TERT HCI','DEC_3P-Penncro TA_HCI','DEC_3P-Quest Diagnostics TA HCI',
'DEC_3P-Cox TA HCI','OP_TA_Bertlesmann HCI','Cap One Card TA HCI 382','RGS_JAX_LabCorp_TA_HCI',
'Cap One Card TA HCI 380','CavalryMain Preview_TA 347','RGS_JAX_JeffCap_TA_HCI','RGS_JAX_Ally_TA_HCI',
'DEC_3P-CLink TA HCI 1','DEC_3P-Ally_TA_HCI','DEC_3P-JFC TA HCI','CavalryMain HCI_TA 347',
'OP_TA-SMC HCI','RGS_JAX_Bureaus TA_HCI','DEC_3P-CLink TA HCI 2') and BALHIT=0 then 0 else BALOPP end as BALOPP,
INCTR,CLNTID 
INTO dbo.#Results_16 
from dbo.#Results_15

--Voicemail call dispoed as TPC calculation
IF OBJECT_ID('tempdb.dbo.#Results_17') IS NOT NULL 
DROP TABLE dbo.#Results_17
select a.EOM,a.AGNTID,a.CALLID,a.CALLDT,a.RECDP,
a.RGSACC,a.DIS1,a.SKNM,a.CLDUR,a.RGSSEID,
a.PHNUMB,a.DIS1CAT,a.CallCDe,a.DIR,a.CALLCNT,
a.MRDHIT3,a.MMHIT1,a.CRDHIT1,a.BALHIT,a.STTIME,
a.MRDOPP,a.MMOPP,a.CRDOPP,a.BALOPP,
case when a.DIS1CAT='TPC' and a.MRDHIT3=0 and 
b.CALLID is not null then 1 else INCTR end as INCTR,CLNTID  
INTO dbo.#Results_17 
from dbo.#Results_16 a left join dbo.#Results1_2_1 b 
on a.CALLID=b.CALLID

--- calculating opportunity based of disposition code (DNC, HUNGUP & DECEASED)
IF OBJECT_ID('tempdb.dbo.#Results_18') IS NOT NULL 
DROP TABLE dbo.#Results_18
select EOM,AGNTID,CALLID,CALLDT,RECDP,RGSACC,DIS1,SKNM,CLDUR,RGSSEID,
PHNUMB,DIS1CAT,CallCDe,DIR,CALLCNT,MRDHIT3,MMHIT1,CRDHIT1,BALHIT,STTIME,
MRDOPP,
case when upper(DIS1) like ('%DNC%') or 
upper(DIS1) like ('%HUNG%')  or upper(DIS1) like ('%DECEASED%') and MMHIT1=0 then 0 else MMOPP end as MMOPP,
case when upper(DIS1) like ('%DNC%') or 
upper(DIS1) like ('%HUNG%') or upper(DIS1) like ('%DECEASED%') and CRDHIT1=0 then 0 else CRDOPP end as CRDOPP,
case when upper(DIS1) like ('%DNC%') or 
upper(DIS1) like ('%HUNG%') or upper(DIS1) like ('%DECEASED%') and BALHIT=0 then 0 else BALOPP end as BALOPP,
INCTR,CLNTID 
INTO dbo.#Results_18 
from dbo.#Results_17 

--if hit then opportunity is yes calculation
IF OBJECT_ID('tempdb.dbo.#Results_19') IS NOT NULL 
DROP TABLE dbo.#Results_19
select EOM,AGNTID,CALLID,CALLDT,RECDP,RGSACC,DIS1,SKNM,CLDUR,RGSSEID,
PHNUMB,DIS1CAT,CallCDe,DIR,CALLCNT,MRDHIT3,MMHIT1,CRDHIT1,BALHIT,STTIME,
MRDOPP,
case when MMOPP=0 and MMHIT1=1 then 1 else MMOPP end as MMOPP,
case when CRDOPP=0 and CRDHIT1=1 then 1 else CRDOPP end as CRDOPP,
case when BALOPP=0 and BALHIT=1 then 1 else BALOPP end as BALOPP,
INCTR,CLNTID
INTO dbo.#Results_19
from dbo.#Results_18 

IF OBJECT_ID('tempdb.dbo.#Results_20') IS NOT NULL 
DROP TABLE dbo.#Results_20
select a.EOM,a.AGNTID,a.CALLID,a.CALLDT,a.RECDP,
a.RGSACC,a.DIS1,a.SKNM,a.CLDUR,a.RGSSEID,a.
PHNUMB,a.DIS1CAT,a.CallCDe,a.DIR,a.CALLCNT,
a.MRDHIT3,a.MMHIT1,a.CRDHIT1,a.BALHIT,a.STTIME,a.
MRDOPP,a.MMOPP,a.CRDOPP,a.BALOPP,a.INCTR,a.CLNTID
INTO dbo.#Results_20
from dbo.#Results_19 a left join dbo.#Results1_2_2 b 
on a.CALLID=b.CALLID
where b.CALLID is null

DELETE FROM dbo.CM_CMPFLDREP
WHERE EOM=@End_Month
--inserting the ouput for reporting purpose
INSERT INTO dbo.CM_CMPFLDREP
(EOM,AGNTID,CALLID,CALLDT,RECDP,RGSACC,DIS1,SKNM,CLDUR,RGSSEID,
PHNUMB,DIS1CAT,CallCDe,DIR,CALLCNT,MRDHIT3,MMHIT1,CRDHIT1,
BALHIT,STTIME,MRDOPP,MMOPP,CRDOPP,BALOPP,INCTR,ClientId)
SELECT EOM,AGNTID,CALLID,CALLDT,RECDP,RGSACC,DIS1,SKNM,CLDUR,RGSSEID,
PHNUMB,CASE when DIS1CAT='RPC' THEN 'Right Party'
when DIS1CAT='PTP' THEN 'Promise to Pay'
when DIS1CAT='TPC' THEN 'Third Party' else 'VoiceMail' end as DIS1CAT ,
CASE when CallCDe='DNERPC250' THEN 'RPC 180-250'
when CallCDe='DNERPC400' THEN 'RPC 251-400'
when CallCDe='DNERPC600' THEN 'RPC 600+'
when CallCDe='DNEPTP' THEN 'Promise to Pay'
when CallCDe='DNETHRDP' THEN 'Third Party' else 'VoiceMail' end as CallCDe,DIR,CALLCNT,MRDHIT3,MMHIT1,CRDHIT1,
BALHIT,ROUND(STTIME/1000,0) as STTIME,MRDOPP,MMOPP,CRDOPP,BALOPP,INCTR,CLNTID
FROM dbo.#Results_20
WHERE UPPER(AGNTID) NOT LIKE '%INTERAC%'


DECLARE @tab char(1) = CHAR(9)

DECLARE @totcntmm VARCHAR (MAX);
SELECT @totcntmm=CAST(COUNT(CALLID) AS varchar) FROM dbo.#Results_20
PRINT @totcntmm

DECLARE @query1 VARCHAR (MAX); 
		SET @query1 = 'select *  from dbo.CM_CMPFLDREP WHERE 
		EOM='+''''+convert(varchar,@End_Month,23)+'''';
PRINT @query1

DECLARE @body1 VARCHAR (MAX); 
		SET @body1 = 'Hi All,
		
Compliance First Line of Defense Report SQL code executed for '+convert(varchar,@Call_Month,102)+'

Total count of call ids added are '+ (@totcntmm) +'. 

Regards,
Business Analytics';
		print @body1
DECLARE @subject1 VARCHAR (MAX); 
		SET @subject1 = isnull(@prefix_subject,'') + 'FLD reporting SQL code executed for ' + convert(varchar,@Call_Month,102);
		print @subject1
	--send email
	--if (SELECT count(CALLID) FROM dbo.#Results_13)>0
		EXEC msdb.dbo.sp_send_dbmail
		@profile_name = @mail_profile,
		@from_address ='Reports SpeechAnalytics <reports.speechanalytics@radiusgs.com>',
		@recipients = 'dw@radiusgs.com;business.analytics@radiusgs.com',
		@copy_recipients=@mail_cc,
		@subject = @subject1,
		@body = @body1,
		@query = @query1 ,
		@execute_query_database=@exec_db, 
		@query_result_header=1, @attach_query_result_as_file=1
	   ,@query_attachment_filename='FLD.csv'
	   ,@query_result_separator=@tab
	   ,@query_result_no_padding=1 
	   ,@query_result_width=32767;

END
GO
GRANT VIEW DEFINITION
    ON OBJECT::[dbo].[usp_12_SQL_FLD_Report_Update] TO [CORP\aramugade]
    AS [dbo];


GO
GRANT VIEW DEFINITION
    ON OBJECT::[dbo].[usp_12_SQL_FLD_Report_Update] TO [CORP\tkumar]
    AS [dbo];


GO
GRANT VIEW DEFINITION
    ON OBJECT::[dbo].[usp_12_SQL_FLD_Report_Update] TO [CORP\aughodake]
    AS [dbo];


GO
GRANT VIEW DEFINITION
    ON OBJECT::[dbo].[usp_12_SQL_FLD_Report_Update] TO [CORP\musalunke]
    AS [dbo];


GO
GRANT VIEW DEFINITION
    ON OBJECT::[dbo].[usp_12_SQL_FLD_Report_Update] TO [CORP\pjain]
    AS [dbo];


GO
GRANT VIEW DEFINITION
    ON OBJECT::[dbo].[usp_12_SQL_FLD_Report_Update] TO [CORP\mhuang]
    AS [dbo];

