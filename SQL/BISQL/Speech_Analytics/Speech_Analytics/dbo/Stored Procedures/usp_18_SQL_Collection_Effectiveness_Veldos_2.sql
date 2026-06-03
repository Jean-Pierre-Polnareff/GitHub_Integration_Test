

-- ================================= ============
-- Author:		Vladislav Pilipets
-- Create date: 2022-05-09
-- Description:	Captures post analysis done for agents efforts on call where consumer behavior was showcased
-- =============================================
CREATE PROCEDURE [dbo].[usp_18_SQL_Collection_Effectiveness_Veldos]
	@mail_profile		varchar(50), 
	@mail_to			varchar(1500), 
	@mail_cc			varchar(1500), 
	@exec_date			datetime = null 
AS  
BEGIN

SET NOCOUNT ON;

DECLARE @prefix_subject AS VARCHAR(50) = (SELECT attributes FROM Speech_Analytics.dbo.cm_connattr WHERE parameter = 'prefix' AND isActive = 1) 

--Setting a eomdate for automatic run
DECLARE @End_Month DATETIME
 
IF @exec_date IS NULL 
BEGIN 
	SET @End_Month=EOMONTH(CAST(DATEADD(DAY,-3,GETDATE()) AS DATE),0)
END 
ELSE 
BEGIN 
	SET @End_Month=EOMONTH(CAST(DATEADD(DAY,-3,@exec_date) AS DATE),0)
END 
PRINT @End_Month

DECLARE @Call_date DATETIME
IF @exec_date IS NULL 
BEGIN 
	SET @Call_date=CAST(DATEADD(DAY,-3,GETDATE()) AS DATE)
END 
ELSE 
BEGIN 
	SET @Call_date=CAST(DATEADD(DAY,-3,@exec_date) AS DATE)
END 

PRINT @Call_date

--Extracting client ids from amex table
IF OBJECT_ID('tempdb.dbo.#Results') IS NOT NULL  
DROP TABLE dbo.#Results
SELECT [CustomerId],[ClientId]
INTO dbo.#Results 
FROM [DW_MSTR_DM].[dbo].[DimCustomer]
WHERE [SourceSystem]='AMEX Latitude' AND [ClientId]<>''

--Extracting calls with result code of RPC or PTP from Events table
IF OBJECT_ID('tempdb.dbo.#Results1') IS NOT NULL 
DROP TABLE dbo.#Results1
SELECT CALLID,STTIME,ROW_NUMBER() OVER (PARTITION BY CALLID ORDER BY STTIME) AS rwnumb
INTO dbo.#Results1
FROM dbo.CM_EVNTSAPI
WHERE EOM=@End_Month AND CATHIT='EN All RPC Calls' and CALLDT<=@Call_date

--Unique Calls with Call Set
IF OBJECT_ID('tempdb.dbo.#Results1_1') IS NOT NULL 
DROP TABLE dbo.#Results1_1
SELECT CALLID 
INTO dbo.#Results1_1
FROM dbo.#Results1
WHERE rwnumb=1

--Extracting calls details with result code of RPC or PTP from Events table
IF OBJECT_ID('tempdb.dbo.#Results2') IS NOT NULL 
DROP TABLE dbo.#Results2
SELECT EOM,AGNTID,CALLID,CALLDT,RECDP,RGSACC,DIS1,SKNM,CLDUR,
WRCNT,DIR,RGSSEID,PHNUMB,PAYRCVD
INTO dbo.#Results2
FROM dbo.CM_CALLEXPORT
WHERE EOM=@End_Month AND CALLID IN (SELECT CALLID FROM dbo.#Results1_1) and CALLDT<=@Call_date
and RECDP='Veldos'

--Extracting calls with message machine language from Events table
IF OBJECT_ID('tempdb.dbo.#Results3') IS NOT NULL 
DROP TABLE dbo.#Results3
SELECT CALLID,STTIME,ROW_NUMBER() OVER (PARTITION BY CALLID ORDER BY STTIME) AS rwnumb
INTO dbo.#Results3
FROM dbo.CM_EVNTSAPI
WHERE EOM=@End_Month AND CATHIT='EN Message Machine' 
AND CALLID IN (SELECT CALLID FROM dbo.#Results1_1) and CALLDT<=@Call_date

--Unique calls with language
IF OBJECT_ID('tempdb.dbo.#Results3_1') IS NOT NULL 
DROP TABLE dbo.#Results3_1
SELECT CALLID 
INTO dbo.#Results3_1
FROM dbo.#Results3
WHERE rwnumb=1

--Extracting calls with consumer profile from Events table
IF OBJECT_ID('tempdb.dbo.#Results4') IS NOT NULL 
DROP TABLE dbo.#Results4
SELECT CALLID,REPLACE(REPLACE(CATHIT,' Consumer',''),'EN ','') AS CATHIT,
COMPNAME,STTIME,ROW_NUMBER() OVER (PARTITION BY CALLID ORDER BY STTIME) AS rwnumb
INTO dbo.#Results4
 FROM dbo.CM_EVNTSAPI WHERE EOM=@End_Month AND CATHIT IN ('EN Pitcher Consumer',
'EN Promiser Consumer','EN Controller Consumer','EN Avoider Consumer') 
AND CALLID IN (SELECT CALLID FROM dbo.#Results1_1) and CALLDT<=@Call_date

--Unique Profiles in the call
IF OBJECT_ID('tempdb.dbo.#Results4_1') IS NOT NULL 
DROP TABLE dbo.#Results4_1 
SELECT CALLID,CATHIT,COMPNAME,STTIME 
INTO dbo.#Results4_1 
FROM dbo.#Results4 
WHERE rwnumb=1

--Count of profiles in a call
IF OBJECT_ID('tempdb.dbo.#Results4_2') IS NOT NULL 
DROP TABLE dbo.#Results4_2 
SELECT CALLID,CATHIT,COUNT(CATHIT) AS CNTCATHIT
INTO dbo.#Results4_2
FROM dbo.#Results4
GROUP BY CALLID,CATHIT

--Count of profiles in a call part 2
IF OBJECT_ID('tempdb.dbo.#Results4_3') IS NOT NULL 
DROP TABLE dbo.#Results4_3 
SELECT CALLID,CASE WHEN CATHIT='Pitcher' THEN CNTCATHIT ELSE 0 END AS CNSPITCH,
CASE WHEN CATHIT='Promiser' THEN CNTCATHIT ELSE 0 END AS CNSPROM,
CASE WHEN CATHIT='Controller' THEN CNTCATHIT ELSE 0 END AS CNSCONT,
CASE WHEN CATHIT='Avoider' THEN CNTCATHIT ELSE 0 END AS CNSAVOI
INTO dbo.#Results4_3
FROM dbo.#Results4_2


--Count of profiles in a call part 3
IF OBJECT_ID('tempdb.dbo.#Results4_4') IS NOT NULL 
DROP TABLE dbo.#Results4_4
SELECT CALLID,SUM(CNSPITCH) AS CNSPITCH,SUM(CNSPROM) AS CNSPROM,
SUM(CNSCONT) AS CNSCONT,SUM(CNSAVOI) AS CNSAVOI
INTO dbo.#Results4_4
FROM dbo.#Results4_3
GROUP BY CALLID

--Extracting calls with type of questions from Events table
IF OBJECT_ID('tempdb.dbo.#Results5') IS NOT NULL 
DROP TABLE dbo.#Results5
SELECT CALLID,REPLACE(CATHIT,'EN AM ','') AS CATHIT,
COMPNAME,STTIME
INTO dbo.#Results5
FROM dbo.CM_EVNTSAPI 
WHERE EOM=@End_Month AND CATHIT IN ('EN AM Open Questions','EN AM Close Questions') 
AND CALLID IN (SELECT CALLID FROM dbo.#Results1_1) and CALLDT<=@Call_date

--Extracting transcripts to calculate word count by channel for particular range of time
IF OBJECT_ID('tempdb.dbo.#Results6') IS NOT NULL 
DROP TABLE dbo.#Results6
SELECT CALLID,WORD,SPKR,STTIME
INTO dbo.#Results6
FROM dbo.CM_TRANSAPI 
WHERE EOM=@End_Month AND CALLID IN (SELECT CALLID FROM dbo.#Results1_1)

--Extracting calls with agent efforts language from Events table
IF OBJECT_ID('tempdb.dbo.#Results7') IS NOT NULL 
DROP TABLE dbo.#Results7
SELECT CALLID,CATHIT,COMPNAME,STTIME,
ROW_NUMBER() OVER (PARTITION BY CALLID ORDER BY STTIME DESC) AS rwnumb
INTO dbo.#Results7 
FROM dbo.CM_EVNTSAPI 
WHERE EOM=@End_Month AND CATHIT ='EN AM Efforts' 
AND CALLID IN (SELECT CALLID FROM dbo.#Results1_1) and CALLDT<=@Call_date

--Unique calls with agent eefort
IF OBJECT_ID('tempdb.dbo.#Results7_1') IS NOT NULL 
DROP TABLE dbo.#Results7_1
SELECT CALLID,CATHIT,COMPNAME,STTIME 
INTO dbo.#Results7_1 
FROM dbo.#Results7
WHERE rwnumb=1


--Removing possible incorrect result codes where message machine language s hit
IF OBJECT_ID('tempdb.dbo.#Results8') IS NOT NULL 
DROP TABLE dbo.#Results8
SELECT a.EOM,a.AGNTID,a.CALLID,a.CALLDT,a.RECDP,a.RGSACC,a.DIS1,
a.SKNM,a.CLDUR,a.WRCNT,a.DIR,a.RGSSEID,a.PHNUMB,a.PAYRCVD
INTO dbo.#Results8
FROM dbo.#Results2 a LEFT JOIN dbo.#Results3_1 b ON b.CALLID = a.CALLID
WHERE b.CALLID IS NULL

--Calculating AM Effort data
IF OBJECT_ID('tempdb.dbo.#Results9') IS NOT NULL 
DROP TABLE dbo.#Results9
SELECT a.EOM,a.AGNTID,a.CALLID,a.CALLDT,a.RECDP,a.RGSACC,a.DIS1,
a.SKNM,a.CLDUR,a.WRCNT,a.DIR,a.RGSSEID,a.PHNUMB,a.PAYRCVD,b.CATHIT AS CNS_CAT,
b.COMPNAME AS CNS_COMP,ROUND(b.STTIME/1000,0) AS CNS_TIME,
c.COMPNAME AS AM_COMP,ROUND(c.STTIME/1000,0) AS AM_TIME,
 ROUND(c.STTIME/1000,0)-ROUND(b.STTIME/1000,0) AS TMDIFF,d.[ClientId] AS CLNT_ID,
 1 AS CLLCNT,CASE WHEN a.DIS1 IN ('AGENT - PTP Direct Check','AGENT - CUST RPC PTP 2',
 'AGENT - PTP Credit Card','AGENT - CUST RPC PTP 1','AGENT - CUST RPC PTP 12',
 'AGENT - CUST RPC PTP 13','AGENT - CUST RPC PTP 14','AGENT - CUST RPC PTP 4',
 'AGENT - CUST RPC PTP 11','AGENT - CUST RPC PTP 10','AGENT - CUST RPC PTP 5') 
 THEN 1 ELSE 0 END AS PTP_FLAG,CASE WHEN ROUND(c.STTIME/1000,0)-ROUND(b.STTIME/1000,0)<=120 
 THEN 'A.00-120 Seconds' WHEN ROUND(c.STTIME/1000,0)-ROUND(b.STTIME/1000,0)<=240 
 THEN 'B.121-240 Seconds' WHEN ROUND(c.STTIME/1000,0)-ROUND(b.STTIME/1000,0)<=360 
 THEN 'C.241-360 Seconds' WHEN ROUND(c.STTIME/1000,0)-ROUND(b.STTIME/1000,0)<=480 
 THEN 'D.361-480 Seconds' ELSE 'E.480+ Seconds' END AS TMEDIFFRNG,e.CNSPITCH,
e.CNSPROM,e.CNSCONT,e.CNSAVOI
INTO dbo.#Results9
FROM dbo.#Results8 a LEFT JOIN dbo.#Results4_1 b ON b.CALLID = a.CALLID
LEFT JOIN dbo.#Results7_1 c ON c.CALLID = a.CALLID
LEFT JOIN dbo.#Results d ON  a.RGSACC=CAST(d.[CustomerId] AS NVARCHAR(255))
LEFT JOIN dbo.#Results4_4 e ON e.CALLID = a.CALLID
WHERE b.CATHIT IS NOT NULL AND c.CATHIT IS NOT NULL AND c.STTIME>b.STTIME

--Calculating number of words between Consumer time and Agent effort Time
IF OBJECT_ID('tempdb.dbo.#Results9_1') IS NOT NULL 
DROP TABLE dbo.#Results9_1
SELECT a.CALLID,a.WORD,a.SPKR,a.STTIME
INTO dbo.#Results9_1
FROM dbo.#Results6 a
LEFT JOIN dbo.#Results9 b ON b.CALLID = a.CALLID
WHERE ROUND(a.STTIME/1000,0) BETWEEN b.CNS_TIME AND b.AM_TIME

--Calculating number of words between Consumer time and Agent effort Time P2
IF OBJECT_ID('tempdb.dbo.#Results9_2') IS NOT NULL 
DROP TABLE dbo.#Results9_2
SELECT CALLID,SPKR,COUNT(WORD) WRCNT
INTO dbo.#Results9_2
FROM dbo.#Results9_1
GROUP BY CALLID,SPKR

--Calculating number of words between Consumer time and Agent effort Time P3
IF OBJECT_ID('tempdb.dbo.#Results9_3') IS NOT NULL 
DROP TABLE dbo.#Results9_3
SELECT CALLID,CASE WHEN SPKR='Agent' THEN WRCNT ELSE 0 END AS AMWRCNT,
CASE WHEN SPKR<>'Agent' THEN WRCNT ELSE 0 END AS CNWRCNT
INTO dbo.#Results9_3
FROM dbo.#Results9_2

--Calculating number of words between Consumer time and Agent effort Time Final
IF OBJECT_ID('tempdb.dbo.#Results9_4') IS NOT NULL 
DROP TABLE dbo.#Results9_4
SELECT CALLID,SUM(AMWRCNT) AS AMWRCT,SUM(CNWRCNT) AS CNWRCT
INTO dbo.#Results9_4
FROM dbo.#Results9_3
GROUP BY CALLID

--Calculating number of Questions between Consumer time and Agent effort Time
IF OBJECT_ID('tempdb.dbo.#Results9_5') IS NOT NULL 
DROP TABLE dbo.#Results9_5
SELECT a.CALLID,a.CATHIT,a.STTIME
INTO dbo.#Results9_5
FROM dbo.#Results5 a
LEFT JOIN dbo.#Results9 b ON b.CALLID = a.CALLID
WHERE ROUND(a.STTIME/1000,0) BETWEEN b.CNS_TIME AND b.AM_TIME

--Calculating number of Questions between Consumer time and Agent effort Time P2
IF OBJECT_ID('tempdb.dbo.#Results9_6') IS NOT NULL 
DROP TABLE dbo.#Results9_6
SELECT CALLID,CATHIT,COUNT(CATHIT) QSCNT
INTO dbo.#Results9_6
FROM dbo.#Results9_5
GROUP BY CALLID,CATHIT

--Calculating number of Questions between Consumer time and Agent effort Time P3
IF OBJECT_ID('tempdb.dbo.#Results9_7') IS NOT NULL 
DROP TABLE dbo.#Results9_7
SELECT CALLID,CASE WHEN CATHIT LIKE 'Open%' THEN QSCNT ELSE 0 END AS OPNQCNT,
CASE WHEN CATHIT LIKE 'Close%' THEN QSCNT ELSE 0 END AS CLOQCNT
INTO dbo.#Results9_7
FROM dbo.#Results9_6

--Calculating number of Questions between Consumer time and Agent effort Time Final
IF OBJECT_ID('tempdb.dbo.#Results9_8') IS NOT NULL 
DROP TABLE dbo.#Results9_8
SELECT CALLID,SUM(OPNQCNT) AS OPNQCT,SUM(CLOQCNT) AS CLOQCNT
INTO dbo.#Results9_8
FROM dbo.#Results9_7
GROUP BY CALLID

--Combining Questions and Word count within AM Effort Table
IF OBJECT_ID('tempdb.dbo.#Results9_9') IS NOT NULL 
DROP TABLE dbo.#Results9_9
SELECT a.EOM,a.AGNTID,a.CALLID,a.CALLDT,a.RECDP,a.RGSACC,a.DIS1,a.SKNM,a.CLDUR,
a.WRCNT,a.DIR,a.RGSSEID,a.PHNUMB,a.PAYRCVD,a.CNS_CAT,a.CNS_COMP,a.CNS_TIME,
a.AM_COMP,a.AM_TIME,a.TMDIFF,a.CLNT_ID,a.CLLCNT,a.PTP_FLAG,a.TMEDIFFRNG,
a.CNSPITCH,a.CNSPROM,a.CNSCONT,a.CNSAVOI,
CASE WHEN b.AMWRCT IS NULL THEN 0 ELSE b.AMWRCT END AS AMWRCNT,
CASE WHEN b.CNWRCT IS NULL THEN 0 ELSE b.CNWRCT END AS CNWRCNT,
CASE WHEN c.OPNQCT IS NULL THEN 0 ELSE c.OPNQCT END AS OPNQCNT,
CASE WHEN c.CLOQCNT IS NULL THEN 0 ELSE c.CLOQCNT END AS CLOQCNT
INTO dbo.#Results9_9
FROM dbo.#Results9 a 
LEFT JOIN dbo.#Results9_4 b ON b.CALLID = a.CALLID
LEFT JOIN dbo.#Results9_8 c ON c.CALLID = a.CALLID

--Calculating Consumer Profile data
IF OBJECT_ID('tempdb.dbo.#Results10') IS NOT NULL 
DROP TABLE dbo.#Results10
SELECT a.EOM,a.AGNTID,a.CALLID,a.CALLDT,a.RECDP,a.RGSACC,a.DIS1,
a.SKNM,a.CLDUR,a.WRCNT,a.DIR,a.RGSSEID,a.PHNUMB,a.PAYRCVD,b.CATHIT AS CNS_CAT,
b.COMPNAME AS CNS_COMP,ROUND(b.STTIME/1000,0) AS CNS_TIME,d.[ClientId] AS CLNT_ID,
 1 AS CLLCNT,CASE WHEN a.DIS1 IN ('AGENT - PTP Direct Check','AGENT - CUST RPC PTP 2',
 'AGENT - PTP Credit Card','AGENT - CUST RPC PTP 1','AGENT - CUST RPC PTP 12',
 'AGENT - CUST RPC PTP 13','AGENT - CUST RPC PTP 14','AGENT - CUST RPC PTP 4',
 'AGENT - CUST RPC PTP 11','AGENT - CUST RPC PTP 10','AGENT - CUST RPC PTP 5') 
 THEN 1 ELSE 0 END AS PTP_FLAG,e.CNSPITCH,e.CNSPROM,e.CNSCONT,e.CNSAVOI
 INTO dbo.#Results10
FROM dbo.#Results8 a 
LEFT JOIN dbo.#Results4_1 b ON b.CALLID = a.CALLID
LEFT JOIN dbo.#Results d ON  a.RGSACC=CAST(d.[CustomerId] AS NVARCHAR(255))
LEFT JOIN dbo.#Results4_4 e ON e.CALLID = a.CALLID
WHERE b.CATHIT IS NOT NULL

--Calculating number of words after Consumer time
IF OBJECT_ID('tempdb.dbo.#Results10_1') IS NOT NULL 
DROP TABLE dbo.#Results10_1
SELECT a.CALLID,a.WORD,a.SPKR,a.STTIME
INTO dbo.#Results10_1
FROM dbo.#Results6 a
LEFT JOIN dbo.#Results10 b ON b.CALLID = a.CALLID
WHERE ROUND(a.STTIME/1000,0)>=b.CNS_TIME

--Calculating number of words after Consumer time P2
IF OBJECT_ID('tempdb.dbo.#Results10_2') IS NOT NULL 
DROP TABLE dbo.#Results10_2
SELECT CALLID,SPKR,COUNT(WORD) WRCNT
INTO dbo.#Results10_2
FROM dbo.#Results10_1
GROUP BY CALLID,SPKR

--Calculating number of words after Consumer time P3
IF OBJECT_ID('tempdb.dbo.#Results10_3') IS NOT NULL 
DROP TABLE dbo.#Results10_3
SELECT CALLID,CASE WHEN SPKR='Agent' THEN WRCNT ELSE 0 END AS AMWRCNT,
CASE WHEN SPKR<>'Agent' THEN WRCNT ELSE 0 END AS CNWRCNT
INTO dbo.#Results10_3
FROM dbo.#Results10_2

--Calculating number of words after Consumer time Final
IF OBJECT_ID('tempdb.dbo.#Results10_4') IS NOT NULL 
DROP TABLE dbo.#Results10_4
SELECT CALLID,SUM(AMWRCNT) AS AMWRCT,SUM(CNWRCNT) AS CNWRCT
INTO dbo.#Results10_4
FROM dbo.#Results10_3
GROUP BY CALLID

--Calculating number of Questions after Consumer time
IF OBJECT_ID('tempdb.dbo.#Results10_5') IS NOT NULL 
DROP TABLE dbo.#Results10_5
SELECT a.CALLID,a.CATHIT,a.STTIME
INTO dbo.#Results10_5
FROM dbo.#Results5 a
LEFT JOIN dbo.#Results10 b ON b.CALLID = a.CALLID
WHERE ROUND(a.STTIME/1000,0)>=b.CNS_TIME

--Calculating number of Questions after Consumer time P2
IF OBJECT_ID('tempdb.dbo.#Results10_6') IS NOT NULL 
DROP TABLE dbo.#Results10_6
SELECT CALLID,CATHIT,COUNT(CATHIT) QSCNT
INTO dbo.#Results10_6
FROM dbo.#Results10_5
GROUP BY CALLID,CATHIT

--Calculating number of Questions after Consumer time P3
IF OBJECT_ID('tempdb.dbo.#Results10_7') IS NOT NULL 
DROP TABLE dbo.#Results10_7
SELECT CALLID,CASE WHEN CATHIT LIKE 'Open%' THEN QSCNT ELSE 0 END AS OPNQCNT,
CASE WHEN CATHIT LIKE 'Close%' THEN QSCNT ELSE 0 END AS CLOQCNT
INTO dbo.#Results10_7
FROM dbo.#Results10_6

--Calculating number of Questions after Consumer time Final
IF OBJECT_ID('tempdb.dbo.#Results10_8') IS NOT NULL 
DROP TABLE dbo.#Results10_8
SELECT CALLID,SUM(OPNQCNT) AS OPNQCT,SUM(CLOQCNT) AS CLOQCNT
INTO dbo.#Results10_8
FROM dbo.#Results10_7
GROUP BY CALLID

--Combining Questions & Word Count with Consumer Profile data
IF OBJECT_ID('tempdb.dbo.#Results10_9') IS NOT NULL 
DROP TABLE dbo.#Results10_9
SELECT a.EOM,a.AGNTID,a.CALLID,a.CALLDT,a.RECDP,a.RGSACC,a.DIS1,
a.SKNM,a.CLDUR,a.WRCNT,a.DIR,a.RGSSEID,a.PHNUMB,a.PAYRCVD,a.CNS_CAT,
a.CNS_COMP,a.CNS_TIME,a.CLNT_ID,a.CLLCNT,a.PTP_FLAG,a.CNSPITCH,
a.CNSPROM,a.CNSCONT,a.CNSAVOI,
CASE WHEN b.AMWRCT IS NULL THEN 0 ELSE b.AMWRCT END AS AMWRCNT,
CASE WHEN b.CNWRCT IS NULL THEN 0 ELSE b.CNWRCT END AS CNWRCNT,
CASE WHEN c.OPNQCT IS NULL THEN 0 ELSE c.OPNQCT END AS OPNQCNT,
CASE WHEN c.CLOQCNT IS NULL THEN 0 ELSE c.CLOQCNT END AS CLOQCNT
INTO dbo.#Results10_9
FROM dbo.#Results10 a 
LEFT JOIN dbo.#Results10_4 b ON b.CALLID = a.CALLID
LEFT JOIN dbo.#Results10_8 c ON c.CALLID = a.CALLID

--Final Table for updation by end of month date Agent Effort
DELETE FROM dbo.CM_AMEFFREPFNL
WHERE EOM=@End_Month
INSERT INTO dbo.CM_AMEFFREPFNL
(EOM,AGNTID,CALLID,CALLDT,RECDP,RGSACC,DIS1,SKNM,CLDUR,WRCNT,
DIR,RGSSEID,PHNUMB,PAYRCVD,CNS_CAT,CNS_COMP,CNS_TIME,AM_COMP,AM_TIME,
TMDIFF,CLNT_ID,CLLCNT,PTP_FLAG,TMEDIFFRNG,CNSPITCH,CNSPROM,CNSCONT,
CNSAVOI,AMWRCNT,CNWRCNT,OPNQCNT,CLOQCNT)
SELECT EOM,AGNTID,CALLID,CALLDT,RECDP,RGSACC,DIS1,SKNM,CLDUR,WRCNT,
DIR,RGSSEID,PHNUMB,PAYRCVD,CNS_CAT,CNS_COMP,CNS_TIME,AM_COMP,AM_TIME,
TMDIFF,CLNT_ID,CLLCNT,PTP_FLAG,TMEDIFFRNG,CNSPITCH,CNSPROM,CNSCONT,
CNSAVOI,AMWRCNT,CNWRCNT,OPNQCNT,CLOQCNT
FROM dbo.#Results9_9

--Final Table for updation by end of month date Consumer Profile Effort
DELETE FROM dbo.CM_CNSPROREPFNL
WHERE EOM=@End_Month
INSERT INTO dbo.CM_CNSPROREPFNL
(EOM,AGNTID,CALLID,CALLDT,RECDP,RGSACC,DIS1,SKNM,CLDUR,WRCNT,
DIR,RGSSEID,PHNUMB,PAYRCVD,CNS_CAT,CNS_COMP,CNS_TIME,CLNT_ID,CLLCNT,
PTP_FLAG,CNSPITCH,CNSPROM,CNSCONT,CNSAVOI,AMWRCNT,CNWRCNT,OPNQCNT,CLOQCNT)
SELECT EOM,AGNTID,CALLID,CALLDT,RECDP,RGSACC,DIS1,SKNM,CLDUR,WRCNT,
DIR,RGSSEID,PHNUMB,PAYRCVD,CNS_CAT,CNS_COMP,CNS_TIME,CLNT_ID,CLLCNT,
PTP_FLAG,CNSPITCH,CNSPROM,CNSCONT,CNSAVOI,AMWRCNT,CNWRCNT,OPNQCNT,CLOQCNT
FROM dbo.#Results10_9

DECLARE @totcnt VARCHAR (MAX);
SELECT @totcnt=CAST(COUNT(CALLID) AS varchar) FROM dbo.#Results9_9
PRINT @totcnt

DECLARE @totcnt1 VARCHAR (MAX);
SELECT @totcnt1=CAST(COUNT(CALLID) AS varchar) FROM dbo.#Results10_9
PRINT @totcnt1

DECLARE @body1 VARCHAR (MAX); 
		SET @body1 = 'Hi All,
		
Collection Effectiveness raw update on Veldos Portal executed for '+convert(varchar,getdate(),23)+'.
		
Total count of calls with Agent effort Analysis are '+ (@totcnt) +' and total count of calls with Consumer profile Analysis are '+ (@totcnt1) +'. 
		

Regards,
Business Analytics';
		print @body1
DECLARE @subject1 VARCHAR (MAX); 
		SET @subject1 = isnull(@prefix_subject,'') + 'Collection Effectiveness(Veldos) raw update executed for ' + convert(varchar,getdate(),23);
		print @subject1
	--send email
	if (SELECT count(CALLID) FROM dbo.#Results9_9)>0
		EXEC msdb.dbo.sp_send_dbmail
		@profile_name = @mail_profile,
		@from_address ='Reports SpeechAnalytics <reports.speechanalytics@radiusgs.com>',
		@recipients='dw@radiusgs.com;business.analytics@radiusgs.com',

		@copy_recipients=@mail_cc,

		@subject = @subject1,

		@body = @body1;

END
GO
GRANT VIEW DEFINITION
    ON OBJECT::[dbo].[usp_18_SQL_Collection_Effectiveness_Veldos] TO [CORP\aramugade]
    AS [dbo];


GO
GRANT VIEW DEFINITION
    ON OBJECT::[dbo].[usp_18_SQL_Collection_Effectiveness_Veldos] TO [CORP\tkumar]
    AS [dbo];


GO
GRANT VIEW DEFINITION
    ON OBJECT::[dbo].[usp_18_SQL_Collection_Effectiveness_Veldos] TO [CORP\aughodake]
    AS [dbo];


GO
GRANT VIEW DEFINITION
    ON OBJECT::[dbo].[usp_18_SQL_Collection_Effectiveness_Veldos] TO [CORP\musalunke]
    AS [dbo];


GO
GRANT VIEW DEFINITION
    ON OBJECT::[dbo].[usp_18_SQL_Collection_Effectiveness_Veldos] TO [CORP\pjain]
    AS [dbo];


GO
GRANT VIEW DEFINITION
    ON OBJECT::[dbo].[usp_18_SQL_Collection_Effectiveness_Veldos] TO [CORP\mhuang]
    AS [dbo];

