-- =============================================
-- Author:		Vladislav Pilipets
-- Create date: 2022-05-09
-- Description:	CM_CALLEXPORT sampling updates 
-- =============================================
CREATE PROCEDURE [dbo].[usp_02_SQL_Compliance_Sampling]
	@mail_profile		varchar(50), 
	@mail_to			varchar(1500), 
	@mail_cc			varchar(1500),  
	@exec_date			datetime = null 
AS
BEGIN

	SET NOCOUNT ON; 

	DECLARE @prefix_subject AS VARCHAR(50) = (SELECT attributes FROM Speech_Analytics.dbo.cm_connattr WHERE parameter = 'prefix' AND isActive = 1) 

	--CALL SAMPLING

	--Setting a eomdate for automatic run
	DECLARE @Exe_Date DATETIME
	SET @Exe_Date=GETDATE()-1
	PRINT @Exe_Date

	--Setting a eomdate for automatic run
	DECLARE @End_Month DATETIME
	SET @End_Month=EOMONTH(CAST(DATEADD(DAY,0,@Exe_Date) AS DATE ),0)
	--SET @End_Month=EOMONTH(CAST(DATEADD(DAY,-3,GETDATE()) AS DATE ),0)
	PRINT @End_Month

	--Setting a 48hr  for automatic run
	DECLARE @Call_Month DATETIME
	SET @Call_Month=CAST(@Exe_Date-1 AS DATE)
	PRINT @Call_Month

	--get unique agent ID 
	IF OBJECT_ID('tempdb.dbo.#Results') IS NOT NULL 
	DROP TABLE dbo.#Results
	SELECT DISTINCT AGNTID 
	INTO dbo.#Results
	FROM dbo.CM_CALLEXPORT with (nolock) 
	WHERE EOM=@End_Month and UPPER(AGNTID) not LIKE '%INTERAC%'

	--creating target tables
	IF OBJECT_ID('tempdb.dbo.#Results_1') IS NOT NULL 
	DROP TABLE dbo.#Results_1
	SELECT AGNTID,2 AS TARRPC250,2 AS TARRPC400,1 AS TARRPC600,
	1 AS TARPTP,2 AS TARTHRDP,2 AS TARLM,10 as TOTTAR
	INTO dbo.#Results_1
	FROM dbo.#Results

	--get all calls for agent ID
	IF OBJECT_ID('tempdb.dbo.#Results_2') IS NOT NULL 
	DROP TABLE dbo.#Results_2
	SELECT DISTINCT a.CALLID,a.AGNTID,a.RECDP,a.CLDUR,a.DIS1,a.CALLDT,b.DIS1CAT
	INTO dbo.#Results_2
	FROM dbo.CM_CALLEXPORT a  with (nolock) LEFT JOIN
	dbo.CM_LVCDE b  with (nolock) on a.DIS1=b.DIS1 and a.RECDP=b.RECDP
	WHERE a.EOM=@End_Month AND a.CALLDT BETWEEN @Call_Month and  @Exe_Date
	and b.DIS1CAT IS NOT NULL and UPPER(a.SKNM) not LIKE '%INTERAC%'

	--get calls already samplled for agent ID
	IF OBJECT_ID('tempdb.dbo.#Results_3') IS NOT NULL 
	DROP TABLE dbo.#Results_3
	SELECT DISTINCT a.CALLID,a.AGNTID,a.RECDP,a.CLDUR,a.DIS1,a.CALLDT,b.DIS1CAT,
	case when b.DIS1CAT='RPC' and a.CLDUR between 180 and 250 then 1 else 0 end as DNERPC250,
	case when b.DIS1CAT='RPC' and a.CLDUR between 251 and 400 then 1 else 0 end as DNERPC400,
	case when b.DIS1CAT='RPC' and a.CLDUR>=600 then 1 else 0 end as DNERPC600,
	case when b.DIS1CAT='PTP' then 1 else 0 end as DNEPTP,
	case when b.DIS1CAT='TPC' then 1 else 0 end as DNETHRDP,
	case when b.DIS1CAT='LM' then 1 else 0 end as DNELM
	INTO dbo.#Results_3
	FROM dbo.CM_CALLEXPORT a LEFT JOIN
	dbo.CM_LVCDE b on a.DIS1=b.DIS1 and a.RECDP=b.RECDP
	WHERE a.EOM=@End_Month AND a.SMPLFLG=1 


	--calculating total sampled calls by agent MTD
	IF OBJECT_ID('tempdb.dbo.#Results_3_1') IS NOT NULL 
	DROP TABLE dbo.#Results_3_1
	SELECT AGNTID,SUM(DNERPC250) AS DNERPC250,SUM(DNERPC400) AS DNERPC400,
	SUM(DNERPC600) AS DNERPC600,SUM(DNEPTP) AS DNEPTP,SUM(DNETHRDP) AS DNETHRDP,
	SUM(DNELM) AS DNELM
	INTO dbo.#Results_3_1
	FROM dbo.#Results_3
	GROUP BY AGNTID

	--calculating unique agent calls sampled in last 48 hrs
	IF OBJECT_ID('tempdb.dbo.#Results_3_2') IS NOT NULL 
	DROP TABLE dbo.#Results_3_2
	SELECT AGNTID
	INTO dbo.#Results_3_2
	FROM dbo.#Results_3
	where CALLDT BETWEEN @Call_Month and  @Exe_Date


	--Calculating reamining calls against target
	IF OBJECT_ID('tempdb.dbo.#Results_3_3') IS NOT NULL 
	DROP TABLE dbo.#Results_3_3
	SELECT a.AGNTID,a.TARRPC250,a.TARRPC400,a.TARRPC600,
	a.TARPTP,a.TARTHRDP,a.TARLM,a.TOTTAR,ISNULL(b.DNERPC250,0)DNERPC250,
	ISNULL(b.DNERPC400,0)DNERPC400,
	ISNULL(b.DNERPC600,0)DNERPC600,ISNULL(b.DNEPTP,0)DNEPTP,
	ISNULL(b.DNETHRDP,0)DNETHRDP,ISNULL(b.DNELM,0)DNELM, 
	(ISNULL(b.DNERPC250,0)+ISNULL(b.DNERPC400,0)+ISNULL(b.DNERPC600,0)
	+ISNULL(b.DNEPTP,0)+ISNULL(b.DNETHRDP,0)+ISNULL(b.DNELM,0)) as TOTDNE,
	(a.TARRPC250-ISNULL(b.DNERPC250,0)) as LFTRPC250,(a.TARRPC400-ISNULL(b.DNERPC400,0)) as LFTRPC400,
	(a.TARRPC600-ISNULL(b.DNERPC600,0))as LFTRPC600,(a.TARPTP-ISNULL(b.DNEPTP,0)) as LFTPTP,
	(a.TARTHRDP-ISNULL(b.DNETHRDP,0)) as LFTTHRDP,(a.TARLM-ISNULL(b.DNELM,0)) as LFTLM
	INTO dbo.#Results_3_3
	FROM dbo.#Results_1 a left join dbo.#Results_3_1 b on 
	a.AGNTID=b.AGNTID

	--removing sammpled agents in last 48 hrs
	IF OBJECT_ID('tempdb.dbo.#Results_4') IS NOT NULL 
	DROP TABLE dbo.#Results_4
	SELECT a.CALLID,a.AGNTID,a.RECDP,a.CLDUR,a.CALLDT,a.DIS1CAT,
	case when a.DIS1CAT='RPC' and a.CLDUR between 180 and 250 THEN 'RPC250'
	 when a.DIS1CAT='RPC' and a.CLDUR between 251 and 400 then 'RPC400'
	 when a.DIS1CAT='RPC' and a.CLDUR>=600 then 'RPC600'
	 when a.DIS1CAT='PTP' then 'PTP' when a.DIS1CAT='TPC' then 'THRDP'
	when a.DIS1CAT='LM' then 'LM' ELSE 'DELETE' END AS CLLTYP
	INTO dbo.#Results_4
	FROM dbo.#Results_2 a LEFT JOIN
	dbo.#Results_3_2 b on a.AGNTID=b.AGNTID
	WHERE b.AGNTID is null 

	--removing sammpled agents in last 48 hrs
	IF OBJECT_ID('tempdb.dbo.#Results_4_1_1') IS NOT NULL 
	DROP TABLE dbo.#Results_4_1_1
	SELECT a.CALLID,a.AGNTID,a.RECDP,a.CLDUR,a.CALLDT,a.DIS1CAT,
	a.CLLTYP,C.LFTRPC250,C.LFTRPC400,C.LFTRPC600,C.LFTPTP,C.LFTTHRDP,C.LFTLM
	INTO dbo.#Results_4_1_1
	FROM dbo.#Results_4 a LEFT JOIN
	dbo.#Results_3_3 C on a.AGNTID=C.AGNTID 
	WHERE a.CLLTYP<>'DELETE'



	--distinct agent id to calculate the call numbers for sampling
	IF OBJECT_ID('tempdb.dbo.#Results_1_4') IS NOT NULL 
	DROP TABLE dbo.#Results_1_4
	SELECT distinct AGNTID
	INTO dbo.#Results_1_4
	FROM dbo.#Results_4_1_1 

	--Setting number of calls required for RPC250
	DECLARE @Req_RPC250 int
	SET @Req_RPC250=(select round(count(*)*.2,0) as numbrpc250 from dbo.#Results_1_4)
	PRINT @Req_RPC250
	--Setting number of calls required for RPC400
	DECLARE @Req_RPC400 int
	SET @Req_RPC400=(select round(count(*)*.2,0) as numbrpc400 from dbo.#Results_1_4)
	PRINT @Req_RPC400
	--Setting number of calls required for RPC600
	DECLARE @Req_RPC600 int
	SET @Req_RPC600=(select round(count(*)*.1,0) as numbrpc600 from dbo.#Results_1_4)
	PRINT @Req_RPC600
	--Setting number of calls required for PTP
	DECLARE @Req_PTP int
	SET @Req_PTP=(select round(count(*)*.1,0) as numbptp from dbo.#Results_1_4)
	PRINT @Req_PTP
	--Setting number of calls required for TPC
	DECLARE @Req_TPC int
	SET @Req_TPC=(select round(count(*)*.2,0) as numbtpc from dbo.#Results_1_4)
	PRINT @Req_TPC
	--Setting number of calls required for LM
	DECLARE @Req_LM int
	SET @Req_LM=(select (count(*)-(@Req_RPC250+@Req_RPC400+@Req_RPC600+@Req_PTP+@Req_TPC)) as numbtpc from dbo.#Results_1_4)
	PRINT @Req_LM

	IF OBJECT_ID('tempdb.dbo.#Results_4_1') IS NOT NULL 
	DROP TABLE dbo.#Results_4_1
	SELECT CALLID,CLLTYP,case when CLLTYP='RPC250' AND LFTRPC250>0 THEN 'K'
	WHEN CLLTYP='RPC400' AND LFTRPC400>0 THEN 'K'
	WHEN CLLTYP='RPC600' AND LFTRPC600>0 THEN 'K'
	WHEN CLLTYP='PTP' AND LFTPTP>0 THEN 'K'
	WHEN CLLTYP='THRDP' AND LFTTHRDP>0 THEN 'K'
	WHEN CLLTYP='LM' AND LFTLM>0 THEN 'K' ELSE 'D' END AS ELGCALL 
	INTO dbo.#Results_4_1
	FROM dbo.#Results_4_1_1


	IF OBJECT_ID('tempdb.dbo.#Results_4_2') IS NOT NULL 
	DROP TABLE dbo.#Results_4_2
	SELECT a.CALLID,a.AGNTID,a.RECDP,a.CLDUR,a.CALLDT,A.CLLTYP
	INTO dbo.#Results_4_2
	FROM dbo.#Results_4 a LEFT JOIN dbo.#Results_4_1 B ON A.CALLID=B.CALLID
	WHERE B.ELGCALL <>'D' --and b.CALLID is null


	IF OBJECT_ID('tempdb.dbo.#Results_4_3_1') IS NOT NULL 
	DROP TABLE dbo.#Results_4_3_1
	SELECT TOP(@Req_RPC250) CALLID,AGNTID,RECDP,CLDUR,CALLDT,CLLTYP
	INTO dbo.#Results_4_3_1
	FROM (SELECT CALLID,AGNTID,RECDP,CLDUR,CALLDT,CLLTYP,
	ROW_NUMBER() OVER (PARTITION BY AGNTID ORDER BY NEWID()) AS RecID FROM dbo.#Results_4_2 
	WHERE CLLTYP='RPC250') A
	WHERE A.RecID=1
	ORDER BY NEWID()

	IF OBJECT_ID('tempdb.dbo.#Results_4_2_1') IS NOT NULL 
	DROP TABLE dbo.#Results_4_2_1
	SELECT a.CALLID,a.AGNTID,a.RECDP,a.CLDUR,a.CALLDT,A.CLLTYP,b.AGNTID AS CKEH1
	INTO dbo.#Results_4_2_1
	FROM dbo.#Results_4_2 a LEFT JOIN dbo.#Results_4_3_1 B ON A.AGNTID=B.AGNTID
	WHERE B.AGNTID is null

	IF OBJECT_ID('tempdb.dbo.#Results_4_3_2') IS NOT NULL 
	DROP TABLE dbo.#Results_4_3_2
	SELECT TOP (@Req_RPC400) CALLID,AGNTID,RECDP,CLDUR,CALLDT,CLLTYP
	INTO dbo.#Results_4_3_2
	FROM (SELECT CALLID,AGNTID,RECDP,CLDUR,CALLDT,CLLTYP,
	ROW_NUMBER() OVER (PARTITION BY AGNTID ORDER BY NEWID()) AS RecID FROM dbo.#Results_4_2_1
	WHERE CLLTYP='RPC400') A
	WHERE A.RecID=1
	ORDER BY NEWID()

	IF OBJECT_ID('tempdb.dbo.#Results_4_2_2') IS NOT NULL 
	DROP TABLE dbo.#Results_4_2_2
	SELECT a.CALLID,a.AGNTID,a.RECDP,a.CLDUR,a.CALLDT,A.CLLTYP,b.AGNTID AS CKEH1
	INTO dbo.#Results_4_2_2
	FROM dbo.#Results_4_2_1 a LEFT JOIN dbo.#Results_4_3_2 B ON A.AGNTID=B.AGNTID
	WHERE B.AGNTID is null

	IF OBJECT_ID('tempdb.dbo.#Results_4_3_3') IS NOT NULL 
	DROP TABLE dbo.#Results_4_3_3
	SELECT TOP (@Req_RPC600) CALLID,AGNTID,RECDP,CLDUR,CALLDT,CLLTYP
	INTO dbo.#Results_4_3_3
	FROM (SELECT CALLID,AGNTID,RECDP,CLDUR,CALLDT,CLLTYP,
	ROW_NUMBER() OVER (PARTITION BY AGNTID ORDER BY NEWID()) AS RecID FROM dbo.#Results_4_2_2
	WHERE CLLTYP='RPC600') A
	WHERE A.RecID=1
	ORDER BY NEWID()

	IF OBJECT_ID('tempdb.dbo.#Results_4_2_3') IS NOT NULL 
	DROP TABLE dbo.#Results_4_2_3
	SELECT a.CALLID,a.AGNTID,a.RECDP,a.CLDUR,a.CALLDT,A.CLLTYP,b.AGNTID AS CKEH1
	INTO dbo.#Results_4_2_3
	FROM dbo.#Results_4_2_2 a LEFT JOIN dbo.#Results_4_3_3 B ON A.AGNTID=B.AGNTID
	WHERE B.AGNTID is null

	IF OBJECT_ID('tempdb.dbo.#Results_4_3_4') IS NOT NULL 
	DROP TABLE dbo.#Results_4_3_4
	SELECT TOP (@Req_PTP) CALLID,AGNTID,RECDP,CLDUR,CALLDT,CLLTYP
	INTO dbo.#Results_4_3_4
	FROM (SELECT CALLID,AGNTID,RECDP,CLDUR,CALLDT,CLLTYP,
	ROW_NUMBER() OVER (PARTITION BY AGNTID ORDER BY NEWID()) AS RecID FROM dbo.#Results_4_2_3
	WHERE CLLTYP='PTP') A
	WHERE A.RecID=1
	ORDER BY NEWID()

	IF OBJECT_ID('tempdb.dbo.#Results_4_2_4') IS NOT NULL 
	DROP TABLE dbo.#Results_4_2_4
	SELECT a.CALLID,a.AGNTID,a.RECDP,a.CLDUR,a.CALLDT,A.CLLTYP,b.AGNTID AS CKEH1
	INTO dbo.#Results_4_2_4
	FROM dbo.#Results_4_2_3 a LEFT JOIN dbo.#Results_4_3_4 B ON A.AGNTID=B.AGNTID
	WHERE B.AGNTID is null


	IF OBJECT_ID('tempdb.dbo.#Results_4_3_5') IS NOT NULL 
	DROP TABLE dbo.#Results_4_3_5
	SELECT TOP (@Req_TPC) CALLID,AGNTID,RECDP,CLDUR,CALLDT,CLLTYP
	INTO dbo.#Results_4_3_5
	FROM (SELECT CALLID,AGNTID,RECDP,CLDUR,CALLDT,CLLTYP,
	ROW_NUMBER() OVER (PARTITION BY AGNTID ORDER BY NEWID()) AS RecID FROM dbo.#Results_4_2_4
	WHERE CLLTYP='THRDP') A
	WHERE A.RecID=1
	ORDER BY NEWID()

	IF OBJECT_ID('tempdb.dbo.#Results_4_2_5') IS NOT NULL 
	DROP TABLE dbo.#Results_4_2_5
	SELECT a.CALLID,a.AGNTID,a.RECDP,a.CLDUR,a.CALLDT,A.CLLTYP,b.AGNTID AS CKEH1
	INTO dbo.#Results_4_2_5
	FROM dbo.#Results_4_2_4 a LEFT JOIN dbo.#Results_4_3_5 B ON A.AGNTID=B.AGNTID
	WHERE B.AGNTID is null

	IF OBJECT_ID('tempdb.dbo.#Results_4_3_6') IS NOT NULL 
	DROP TABLE dbo.#Results_4_3_6
	SELECT TOP (@Req_LM) CALLID,AGNTID,RECDP,CLDUR,CALLDT,CLLTYP
	INTO dbo.#Results_4_3_6
	FROM (SELECT CALLID,AGNTID,RECDP,CLDUR,CALLDT,CLLTYP,
	ROW_NUMBER() OVER (PARTITION BY AGNTID ORDER BY NEWID()) AS RecID FROM dbo.#Results_4_2_5
	WHERE CLLTYP='LM') A
	WHERE A.RecID=1
	ORDER BY NEWID()

	IF OBJECT_ID('tempdb.dbo.#Results_5') IS NOT NULL 
	DROP TABLE dbo.#Results_5
	SELECT a.CALLID
	INTO dbo.#Results_5
	FROM (
	SELECT CALLID FROM dbo.#Results_4_3_1
	union
	SELECT CALLID FROM dbo.#Results_4_3_2
	union
	SELECT CALLID FROM dbo.#Results_4_3_3
	union
	SELECT CALLID FROM dbo.#Results_4_3_4
	union
	SELECT CALLID FROM dbo.#Results_4_3_5
	union
	SELECT CALLID FROM dbo.#Results_4_3_6) a

	IF OBJECT_ID('tempdb.dbo.#Results_6') IS NOT NULL 
	DROP TABLE dbo.#Results_6
	SELECT [CLIENT_ID]
	INTO dbo.#Results_6
	FROM [DW_MSTR_DM].[dbo].[LU_CLIENT]
	WHERE [CLIENT_DESC] Like 'USAA%'

	UPDATE DBO.CM_CALLEXPORT SET SMPLFLG=1
	WHERE CALLID IN (SELECT CALLID FROM DBO.#RESULTS_5)
	AND EOM=@End_Month

	UPDATE DBO.CM_CALLEXPORT SET SMPLFLG=1
	WHERE CLNTID IN (SELECT [CLIENT_ID] FROM DBO.#Results_6)
	AND EOM=@End_Month

	DECLARE @totcnt VARCHAR (MAX);
	SELECT @totcnt=CAST(COUNT(CALLID) AS varchar) FROM dbo.#Results_5
	PRINT @totcnt


	DECLARE @body1 VARCHAR (MAX); 
			SET @body1 = 'Hi All,
		
	Compliance Sampling process executed on SQL Server for '+convert(varchar,@Exe_Date,102)+'.
		
	Total count of call ids added for Compliance Sampling are '+ (@totcnt) +'.

	Regards,
	Business Analytics';
			print @body1
	DECLARE @subject1 VARCHAR (MAX); 
			SET @subject1 = 'SQL Compliance Sampling process executed for ' + convert(varchar,@Exe_Date,102);
			print @subject1
		--send email
		if (SELECT count(CALLID) FROM DBO.#RESULTS_5)>0
			EXEC msdb.dbo.sp_send_dbmail
			@profile_name = 'DW Mail',--@@SERVERNAME, --'DFW2-BISQL-001',
			@from_address ='Reports SpeechAnalytics <reports.speechanalytics@radiusgs.com>',
			@recipients = 
			'Anuradha.Poddar@radiusgs.com;Darpan.Thakkar@radiusgs.com;
			Mukesh.Salunke@radiusgs.com;Neenad.Shinde@radiusgs.com;
			Priyanshi.Mishra@radiusgs.com;Ritesh.Singh@radiusgs.com;Vladislav.Pilipets@radiusgs.com',
			@copy_recipients='Pulkit.Jain@radiusgs.com;',
			@subject = @subject1,
			@body = @body1; 
END
GO
GRANT VIEW DEFINITION
    ON OBJECT::[dbo].[usp_02_SQL_Compliance_Sampling] TO [CORP\aramugade]
    AS [dbo];


GO
GRANT VIEW DEFINITION
    ON OBJECT::[dbo].[usp_02_SQL_Compliance_Sampling] TO [CORP\tkumar]
    AS [dbo];


GO
GRANT VIEW DEFINITION
    ON OBJECT::[dbo].[usp_02_SQL_Compliance_Sampling] TO [CORP\aughodake]
    AS [dbo];


GO
GRANT VIEW DEFINITION
    ON OBJECT::[dbo].[usp_02_SQL_Compliance_Sampling] TO [CORP\musalunke]
    AS [dbo];


GO
GRANT VIEW DEFINITION
    ON OBJECT::[dbo].[usp_02_SQL_Compliance_Sampling] TO [CORP\pjain]
    AS [dbo];


GO
GRANT VIEW DEFINITION
    ON OBJECT::[dbo].[usp_02_SQL_Compliance_Sampling] TO [CORP\mhuang]
    AS [dbo];

