
CREATE PROCEDURE [dbo].[usp_79_SQL_PAYINFO_RAW_AXP]
AS
BEGIN

	DECLARE @Start_Date DATETIME
	SELECT @Start_Date=CAST(DATEADD(DAY,-3,GETDATE()) AS DATE )
	PRINT @Start_Date

	DECLARE @ClStart_Date DATETIME
	SELECT @ClStart_Date=CAST(DATEADD(DAY,1,EOMONTH(@Start_Date,-1)) AS DATE )
	PRINT @ClStart_Date

	DECLARE @End_Date DATETIME
	SELECT @End_Date=EOMONTH(@ClStart_Date,0)
	PRINT @End_Date

	-- Extarcting Payment Info Evenet & Verifier Language event 
	IF OBJECT_ID('tempdb.dbo.#RPCCalls') IS NOT NULL 
	DROP TABLE dbo.#RPCCalls;
	SELECT [EOM]
		  ,[CALLDT]
		  ,[CALLID]
		  ,[CATHIT]
		  ,[COMPNAME]
		  ,[STTIME]
		  ,[ENDTIME]
	INTO dbo.#RPCCalls
	FROM (
	SELECT [EOM]
		  ,[CALLDT]
		  ,[CALLID]
		  ,[CATHIT]
		  ,[COMPNAME]
		  ,ROUND([STTIME]/1000,0) [STTIME]
		  ,ROUND([ENDTIME]/1000,0) [ENDTIME]
	FROM [Speech_Analytics].[dbo].[CM_EVNTSAPI]
	WHERE [CATHIT] IN ('EN AM Payment Information','EN SBB Payment Info','EN Verifier Language')
	AND EOM=@End_Date
	UNION
	SELECT [EOM]
		  ,[CALLDT]
		  ,[CALLID]
		  ,[CATHIT]
		  ,[COMPNAME]
		  ,ROUND([STTIME]/1000,0) [STTIME]
		  ,ROUND([ENDTIME]/1000,0) [ENDTIME]
	FROM [Speech_Analytics].[dbo].[CM_EVNTSAPICMP]
	  WHERE [CATHIT] IN ('EN AM Payment Information','EN SBB Payment Info','EN Verifier Language')
	  AND EOM=@End_Date) A

	--Extarcting Call Details for reporting
	IF OBJECT_ID('tempdb.dbo.#RPCCalls1') IS NOT NULL 
	DROP TABLE dbo.#RPCCalls1;
	SELECT DISTINCT B.[EOM]
		  ,B.[AGNTID]
		  ,B.[CALLID]
		  ,B.[CALLDT]
		  ,B.[RECDP]
		  ,B.[RGSACC]
		  ,B.[DIS1]
		  ,B.CLNTID
		  ,B.[SKNM]
		  ,B.[CSST]
		  ,B.[CLDUR]
		  ,B.[DIR]
		  ,REPLACE(REPLACE(REPLACE(REPLACE(B.RGSSEID,'D.mp3','')
						,'.mp3',''),'N.mp3',''),'_','@') RGSSEID
		  ,B.[PHNUMB]
	INTO dbo.#RPCCalls1
	FROM [Speech_Analytics].[dbo].[CM_CALLEXPORT] B 
	WHERE b.RECDP='Veldos' 
		  AND B.[AGNTID]<>'' 
		  AND B.AGNTID NOT IN (SELECT DISTINCT AGNTID FROM Speech_Analytics.dbo.CM_Dim_AXPLEGAL_AGENT WHERE AGNTROLE='Verifier')
		  AND B.CALLDT>='2025-03-03'
		  AND B.CALLID IN (SELECT DISTINCT CALLID FROM dbo.#RPCCalls WHERE [CATHIT]='EN AM Payment Information')
		  AND B.EOM=@End_Date

	-- Extracting Legal Client IDs from Latitude
	IF OBJECT_ID('tempdb.dbo.#Results') IS NOT NULL 
	DROP TABLE dbo.#Results 
	SELECT DISTINCT LEFT([id1], LEN([id1]) - 1)  as CLNTID
	INTO dbo.#Results
	FROM [HVDB02.CORPGLBDOM.LOCAL].[Amex].[dbo].[master]
	WHERE [Branch]='00001'

	--Extarcting Call Details for reporting
	IF OBJECT_ID('tempdb.dbo.#RPCCalls2') IS NOT NULL 
	DROP TABLE dbo.#RPCCalls2;
	SELECT B.[EOM]
		  ,B.[AGNTID]
		  ,B.[CALLID]
		  ,B.[CALLDT]
		  ,B.[RECDP]
		  ,B.[RGSACC]
		  ,B.[DIS1]
		  ,A.CLNTID
		  ,B.[SKNM]
		  ,B.[CSST]
		  ,B.[CLDUR]
		  ,B.[DIR]
		  ,B.RGSSEID
		  ,B.[PHNUMB]
	INTO dbo.#RPCCalls2
	FROM dbo.#RPCCalls1 B 
		LEFT JOIN dbo.#Results A
			ON B.CLNTID=A.CLNTID
	WHERE A.CLNTID IS NOT NULL

	IF OBJECT_ID('tempdb.dbo.#RPCCalls3') IS NOT NULL 
	DROP TABLE dbo.#RPCCalls3;
	SELECT B.[EOM]
		  ,B.[AGNTID]
		  ,B.[CALLID]
		  ,B.[CALLDT]
		  ,B.[RECDP]
		  ,B.[RGSACC]
		  ,B.[DIS1]
		  ,B.CLNTID
		  ,B.[SKNM]
		  ,B.[CSST]
		  ,B.[CLDUR]
		  ,B.[DIR]
		  ,B.RGSSEID
		  ,B.[PHNUMB]
	INTO dbo.#RPCCalls3
	FROM dbo.#RPCCalls1 B
	WHERE B.SKNM IN (SELECT DISTINCT SKNM FROM dbo.#RPCCalls2)
		  AND B.CLNTID in ('NOTAVAIL','NAPP')

	IF OBJECT_ID('tempdb.dbo.#RPCCallsFnl') IS NOT NULL 
	DROP TABLE dbo.#RPCCallsFnl;
	SELECT B.[EOM]
		  ,B.[AGNTID]
		  ,B.[CALLID]
		  ,B.[CALLDT]
		  ,B.[RECDP]
		  ,B.[RGSACC]
		  ,B.[DIS1]
		  ,B.CLNTID
		  ,B.[SKNM]
		  ,B.[CSST]
		  ,B.[CLDUR]
		  ,B.[DIR]
		  ,B.RGSSEID
		  ,B.[PHNUMB]
	INTO dbo.#RPCCallsFnl
	FROM(
	SELECT B.[EOM]
		  ,B.[AGNTID]
		  ,B.[CALLID]
		  ,B.[CALLDT]
		  ,B.[RECDP]
		  ,B.[RGSACC]
		  ,B.[DIS1]
		  ,B.CLNTID
		  ,B.[SKNM]
		  ,B.[CSST]
		  ,B.[CLDUR]
		  ,B.[DIR]
		  ,B.RGSSEID
		  ,B.[PHNUMB]
	FROM dbo.#RPCCalls2 B
	Union
	SELECT B.[EOM]
		  ,B.[AGNTID]
		  ,B.[CALLID]
		  ,B.[CALLDT]
		  ,B.[RECDP]
		  ,B.[RGSACC]
		  ,B.[DIS1]
		  ,B.CLNTID
		  ,B.[SKNM]
		  ,B.[CSST]
		  ,B.[CLDUR]
		  ,B.[DIR]
		  ,B.RGSSEID
		  ,B.[PHNUMB]
	FROM dbo.#RPCCalls3 B) B

	IF OBJECT_ID('tempdb.dbo.#ACCLIST') IS NOT NULL 
	DROP TABLE dbo.#ACCLIST;
	SELECT [RGSACC]
		  ,ROW_NUMBER() OVER (ORDER BY  [RGSACC]) ACCCOUNT
	INTO dbo.#ACCLIST
	FROM (
	SELECT DISTINCT CASE WHEN ISNUMERIC([RGSACC])=1 THEN [RGSACC] END [RGSACC] 
	FROM dbo.#RPCCallsFnl) A
	WHERE [RGSACC] IS NOT NULL

	IF OBJECT_ID('tempdb.dbo.#AccPND') IS NOT NULL 
	DROP TABLE dbo.#AccPND;
	SELECT [Number]
		  ,CAST([DateValueChanged] AS date) [DateValueChanged1]
		  ,[NewValue]
	INTO dbo.#AccPND
	FROM [HVDB02.CORPGLBDOM.LOCAL].[Amex].[dbo].[custom_amex_export] with (nolock)
	WHERE [number] IN (SELECT DISTINCT [RGSACC] FROM dbo.#ACCLIST)
		AND [NewValue]='PND'
		AND CAST([DateValueChanged] AS date) BETWEEN @ClStart_Date and @End_Date

	-- Ranking Payment Info Evenet to extract unique events
	IF OBJECT_ID('tempdb.dbo.#RPCCalls_1') IS NOT NULL 
	DROP TABLE dbo.#RPCCalls_1;
	SELECT [EOM]
		  ,[CALLDT]
		  ,[CALLID]
		  ,[CATHIT]
		  ,[COMPNAME]
		  ,[STTIME]
		  ,[ENDTIME]
		  ,ROW_NUMBER() OVER (PARTITION BY [CALLID] ORDER BY [STTIME]) RNK
	INTO dbo.#RPCCalls_1
	FROM dbo.#RPCCalls
	WHERE [CATHIT] ='EN AM Payment Information'

	--Extracting Unique Events
	IF OBJECT_ID('tempdb.dbo.#RPCCalls_2') IS NOT NULL 
	DROP TABLE dbo.#RPCCalls_2;
	SELECT [EOM]
		  ,[CALLDT]
		  ,[CALLID]
		  ,[STTIME] AMPYIFOSTTIM
		  ,1 AS CALLCOUNT
	INTO dbo.#RPCCalls_2
	FROM dbo.#RPCCalls_1
	WHERE RNK=1

	-- Ranking Payment Info Evenet to extract unique events
	IF OBJECT_ID('tempdb.dbo.#RPCCalls_1_1') IS NOT NULL 
	DROP TABLE dbo.#RPCCalls_1_1;
	SELECT [EOM]
		  ,[CALLDT]
		  ,[CALLID]
		  ,[CATHIT]
		  ,[COMPNAME]
		  ,[STTIME]
		  ,[ENDTIME]
		  ,ROW_NUMBER() OVER (PARTITION BY [CALLID],[STTIME] ORDER BY [STTIME]) RNK
	INTO dbo.#RPCCalls_1_1
	FROM dbo.#RPCCalls
	WHERE [CATHIT] ='EN SBB Payment Info'

	--Extracting Unique Events
	IF OBJECT_ID('tempdb.dbo.#RPCCalls_2_1') IS NOT NULL 
	DROP TABLE dbo.#RPCCalls_2_1;
	SELECT [EOM]
		  ,[CALLDT]
		  ,[CALLID]
		  ,[STTIME] CXPAYINFOSTTIM
		  ,1 AS CALLCOUNT
	INTO dbo.#RPCCalls_2_1
	FROM dbo.#RPCCalls_1_1
	WHERE RNK=1

	IF OBJECT_ID('tempdb.dbo.#RPCCalls_2_2') IS NOT NULL 
	DROP TABLE dbo.#RPCCalls_2_2;
	SELECT A.EOM
		  ,A.CALLDT
		  ,A.CALLID
		  ,A.AMPYIFOSTTIM
		  ,B.CXPAYINFOSTTIM
		  ,B.CXPAYINFOSTTIM-A.AMPYIFOSTTIM AS TMDIFF
		  ,ROW_NUMBER() OVER(PARTITION BY A.CALLID,A.AMPYIFOSTTIM ORDER BY B.CXPAYINFOSTTIM) RNK
	INTO dbo.#RPCCalls_2_2
	FROM dbo.#RPCCalls_2 A 
		left join dbo.#RPCCalls_2_1 B
	ON A.CALLID=B.CALLID
	WHERE B.CALLID IS NOT NULL
	AND B.CXPAYINFOSTTIM-A.AMPYIFOSTTIM Between 0 and 60
	order by A.CALLID

	IF OBJECT_ID('tempdb.dbo.#RPCCalls_2_3') IS NOT NULL 
	DROP TABLE dbo.#RPCCalls_2_3;
	SELECT A.EOM
		  ,A.CALLDT
		  ,A.CALLID
		  ,A.AMPYIFOSTTIM
		  ,A.CXPAYINFOSTTIM
		  ,A.TMDIFF
	INTO dbo.#RPCCalls_2_3
	FROM dbo.#RPCCalls_2_2 A
	WHERE A.RNK=1

	-- Ranking Payment Info Evenet to extract unique events
	IF OBJECT_ID('tempdb.dbo.#RPCCalls_3') IS NOT NULL 
	DROP TABLE dbo.#RPCCalls_3;
	SELECT [EOM]
		  ,[CALLDT]
		  ,[CALLID]
		  ,[CATHIT]
		  ,[COMPNAME]
		  ,[STTIME]
		  ,[ENDTIME]
		  ,ROW_NUMBER() OVER (PARTITION BY [CALLID] ORDER BY [STTIME]) RNK
	INTO dbo.#RPCCalls_3
	FROM dbo.#RPCCalls
	WHERE [CATHIT] ='EN Verifier Language'

	--Extracting Unique Events
	IF OBJECT_ID('tempdb.dbo.#RPCCalls_4') IS NOT NULL 
	DROP TABLE dbo.#RPCCalls_4;
	SELECT [EOM]
		  ,[CALLDT]
		  ,[CALLID]
		  ,[STTIME] VERIFRLANGSTME
	INTO dbo.#RPCCalls_4
	FROM dbo.#RPCCalls_3
	WHERE RNK=1

	--Extracting Unique Events
	IF OBJECT_ID('tempdb.dbo.#RPCCalls_5') IS NOT NULL 
	DROP TABLE dbo.#RPCCalls_5;
	SELECT A.EOM
		  ,A.CALLDT
		  ,A.CALLID
		  ,A.AMPYIFOSTTIM
		  ,A.CXPAYINFOSTTIM
		  ,A.TMDIFF
		  ,CASE WHEN B.[CALLID] IS NOT NULL THEN 1 ELSE 0 END AS VERFLAG
		  ,CASE WHEN B.[CALLID] IS NOT NULL THEN B.VERIFRLANGSTME END VERIFRLANGSTME
	INTO dbo.#RPCCalls_5
	FROM dbo.#RPCCalls_2_3 A
		LEFT JOIN dbo.#RPCCalls_4 B
			on  A.CALLID=B.CALLID

	--Extracting Unique Events
	IF OBJECT_ID('tempdb.dbo.#RPCCalls_6') IS NOT NULL 
	DROP TABLE dbo.#RPCCalls_6;
	SELECT A.EOM
		  ,A.AGNTID
		  ,A.CALLID
		  ,A.CALLDT
		  ,A.RECDP
		  ,A.RGSACC
		  ,A.DIS1
		  ,A.CLNTID
		  ,A.SKNM
		  ,A.CSST
		  ,A.CLDUR
		  ,A.DIR
		  ,A.RGSSEID
		  ,A.PHNUMB
		  ,A.[NewValue]
	 INTO dbo.#RPCCalls_6
	 FROM 
	 (SELECT A.EOM
		  ,A.AGNTID
		  ,A.CALLID
		  ,A.CALLDT
		  ,A.RECDP
		  ,A.RGSACC
		  ,A.DIS1
		  ,A.CLNTID
		  ,A.SKNM
		  ,A.CSST
		  ,A.CLDUR
		  ,A.DIR
		  ,A.RGSSEID
		  ,A.PHNUMB
		  ,B.[NewValue]
	FROM dbo.#RPCCallsFnl A
		LEFT JOIN dbo.#AccPND B
			ON A.RGSACC=B.[Number]
				AND A.CALLDT=B.[DateValueChanged1]
		WHERE ISNUMERIC(A.RGSACC)=1
	UNION
	SELECT A.EOM
		  ,A.AGNTID
		  ,A.CALLID
		  ,A.CALLDT
		  ,A.RECDP
		  ,A.RGSACC
		  ,A.DIS1
		  ,A.CLNTID
		  ,A.SKNM
		  ,A.CSST
		  ,A.CLDUR
		  ,A.DIR
		  ,A.RGSSEID
		  ,A.PHNUMB 
		  ,NULL AS [NewValue]
	FROM dbo.#RPCCallsFnl A
	WHERE ISNUMERIC(A.RGSACC)=0 ) A

	IF OBJECT_ID('tempdb.dbo.#RPCCalls_7') IS NOT NULL 
	DROP TABLE dbo.#RPCCalls_7;
	SELECT A.EOM
		  ,A.AGNTID
		  ,A.CALLID
		  ,A.CALLDT
		  ,A.RECDP
		  ,A.RGSACC
		  ,A.DIS1
		  ,A.CLNTID
		  ,A.SKNM
		  ,A.CSST
		  ,A.CLDUR
		  ,A.DIR
		  ,A.RGSSEID
		  ,A.PHNUMB
		  ,A.[NewValue]
		  ,B.AMPYIFOSTTIM
		  ,B.VERFLAG
		  ,B.VERIFRLANGSTME
		  ,B.CXPAYINFOSTTIM
		  ,B.TMDIFF
		  ,1 AS CALLCOUNT
		  ,CASE WHEN B.AMPYIFOSTTIM IS NULL THEN 0 else 1 end as AMIFOFLAG
		  ,CASE WHEN A.[NewValue] IS NOT NULL THEN 1 else 0 end as PNDFLAG
		  ,CASE WHEN A.[NewValue] IS NOT NULL THEN 0
				WHEN B.AMPYIFOSTTIM IS NULL THEN 0
				WHEN A.[NewValue] IS NULL AND B.AMPYIFOSTTIM IS NOT NULL  AND B.CXPAYINFOSTTIM-B.VERIFRLANGSTME between 0 and 30 THEN 0
				WHEN A.[NewValue] IS NULL AND B.AMPYIFOSTTIM IS NOT NULL AND B.CXPAYINFOSTTIM-B.VERIFRLANGSTME between 0 and 30 
					 AND B.VERIFRLANGSTME<=B.AMPYIFOSTTIM THEN 0
				ELSE 1 END AS POSSBLEINF
	INTO dbo.#RPCCalls_7
	FROM dbo.#RPCCalls_6 A
		LEFT JOIN dbo.#RPCCalls_5 B
			ON A.CALLID=B.CALLID
	WHERE A.AGNTID IN (SELECT DISTINCT AGNTID FROM Speech_Analytics.dbo.CM_Dim_AXPLEGAL_AGENT WHERE AGNTROLE='Agent')

	DELETE FROM Speech_Analytics.dbo.CM_Rpt_AXPLegalPayInfo
	WHERE EOM=@End_Date

	INSERT INTO Speech_Analytics.dbo.CM_Rpt_AXPLegalPayInfo
	SELECT EOM
		  ,AGNTID
		  ,CALLID
		  ,CALLDT
		  ,RECDP
		  ,RGSACC
		  ,DIS1
		  ,CLNTID
		  ,SKNM
		  ,CSST
		  ,CLDUR
		  ,DIR
		  ,RGSSEID
		  ,PHNUMB
		  ,[NewValue]
		  ,AMPYIFOSTTIM
		  ,VERFLAG
		  ,VERIFRLANGSTME
		  ,CXPAYINFOSTTIM
		  ,TMDIFF
		  ,CALLCOUNT
		  ,POSSBLEINF
		  ,PNDFLAG
		  ,AMIFOFLAG
	FROM dbo.#RPCCalls_7


	DECLARE @tab char(1) = CHAR(9)

	DECLARE @totcntmm VARCHAR (MAX);
	SELECT @totcntmm=CAST(COUNT(CALLID) AS varchar) FROM dbo.#RPCCalls_7
	PRINT @totcntmm

	DECLARE @body1 VARCHAR (MAX); 
			SET @body1 = 'Hi All,
		
	Daily AXP Legal Speech SQL code executed for '+convert(varchar,@End_Date,102)+'

	Total count of rows added are '+ (@totcntmm) +'. 

	Regards,
	Business Analytics';
			print @body1
	DECLARE @subject1 VARCHAR (MAX); 
			SET @subject1 = 'Daily AXP Legal Speech SQL code executed for ' + convert(varchar,@End_Date,102);
			print @subject1
		--send email
		if (SELECT count(CALLID) FROM dbo.#RPCCalls_7)>0
			EXEC msdb.dbo.sp_send_dbmail
			@profile_name = 'DW Mail',--@@SERVERNAME, --'DFW2-BISQL-001',
			@from_address ='Reports SpeechAnalytics <reports.speechanalytics@radiusgs.com>',
			@recipients = 'dw@radiusgs.com;business.analytics@radiusgs.com',
			@copy_recipients='Pulkit.Jain@radiusgs.com;',
			@subject = @subject1,
			@body = @body1;

END