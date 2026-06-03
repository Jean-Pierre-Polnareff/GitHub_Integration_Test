
-- =============================================
-- Author:		Vladislav Pilipets
-- Create date: 2024-10-03
-- Description:	create report for ATT second voice analysis
-- =============================================
CREATE PROCEDURE usp_67_SQL_ATT_SCVoice_Rep 
	
AS
BEGIN
	SET NOCOUNT ON;

	--Setting a eomdate for automatic run
	DECLARE @End_Month DATETIME
	SET @End_Month=EOMONTH(CAST(DATEADD(DAY,-3,GETDATE()) AS DATE ),0)
	PRINT @End_Month

	-- getting at&t actual name
	IF OBJECT_ID('tempdb.dbo.#ResultsM') IS NOT NULL 
	DROP TABLE dbo.#ResultsM
	select * 
	INTO dbo.#ResultsM
	from dbo.CM_ClientParent
	where ClientStream like 'AT&T'

	-- getting at&t client ids based of actual name
	IF OBJECT_ID('tempdb.dbo.#ResultsM1') IS NOT NULL 
	DROP TABLE dbo.#ResultsM1
	SELECT ClientId,SessionName
	INTO dbo.#ResultsM1
	FROM (SELECT ClientId,ClientStream AS SessionName 
	FROM DW_MSTR_DM.dbo.DimClient
	Union
	SELECT CLIENT_ID as ClientId,[Parent] AS SessionName 
	FROM DW_MSTR_DM.dbo.TblClientStreams) a
	where a.SessionName IN (SELECT ClientStream FROM dbo.#ResultsM)

	--get all calls for current month for ATT
	IF OBJECT_ID('tempdb.dbo.#Results1') IS NOT NULL 
	DROP TABLE dbo.#Results1
	SELECT DISTINCT A.EOM
		  ,A.AGNTID
		  ,A.CALLID
		  ,A.CALLDT
		  ,A.RGSSEID
		  ,A.DIS1
		  ,A.RECDP
		  ,A.PHNUMB
		  ,A.CLNTID
		  ,A.SILT
		  ,A.CLDUR
		  ,A.CATCNT
		  ,B.DIS1CAT 
	INTO dbo.#Results1
	FROM [DBO].[CM_CALLEXPORT] A LEFT JOIN
		 [DBO].[CM_LVCDE] B 
			ON A.RECDP=B.RECDP AND A.DIS1=B.DIS1
	WHERE A.EOM=@End_Month 
		  AND ([SMPLFLG] IS NULL OR [SMPLFLG]=0)
		  AND b.DIS1CAT IN ('RPC','PTP') AND UPPER(A.DIR)='OUTBOUND'
		  AND (A.CLNTID IN (SELECT DISTINCT ClientId FROM dbo.#ResultsM1)
	OR A.CLNTID='ATT')
	ORDER BY A.CALLDT

	--Getting Second Voice Category for all Calls
	IF OBJECT_ID('tempdb.dbo.#Results') IS NOT NULL 
	DROP TABLE dbo.#Results
	SELECT EOM
		   ,CALLID
		   ,COMPNAME
		   ,CATHIT
		   ,ROUND(STTIME/1000,0) STTIME
		   ,ROW_NUMBER() OVER (PARTITION BY CALLID,CATHIT 
			ORDER BY STTIME) as RNK1
	  INTO dbo.#Results
	  FROM [dbo].[CM_EVNTSAPI] 
	  where EOM=@End_Month 
			AND CATHIT='EN Second Voice'
			AND CALLID IN (SELECT DISTINCT CALLID FROM dbo.#Results1)

	--Getting Payment verification Category for all Calls
	IF OBJECT_ID('tempdb.dbo.#ResultsPV') IS NOT NULL 
	DROP TABLE dbo.#ResultsPV
	SELECT EOM
		   ,CALLID
		   ,COMPNAME
		   ,CATHIT
		   ,ROUND(STTIME/1000,0) STTIME
		   ,ROW_NUMBER() OVER (PARTITION BY CALLID,CATHIT 
			ORDER BY STTIME) as RNK1
	  INTO dbo.#ResultsPV
	  FROM [dbo].[CM_EVNTSAPI] 
	  where EOM=@End_Month 
			AND CATHIT='EN_Supervisor Payment Script'
			AND CALLID IN (SELECT DISTINCT CALLID FROM dbo.#Results1)
			AND COMPNAME NOT IN ('Paid Letter','Processing Fee')

	--Getting Second Voice Category for all Calls
	IF OBJECT_ID('tempdb.dbo.#Results_1') IS NOT NULL 
	DROP TABLE dbo.#Results_1
	SELECT EOM
		  ,CALLID
		  ,COMPNAME
		  ,CATHIT
		  ,ROUND(STTIME/1000,0) STTIME
		  ,ROW_NUMBER() OVER (PARTITION BY CALLID,CATHIT 
			ORDER BY STTIME) as RNK1
	  INTO dbo.#Results_1
	  FROM [dbo].[CM_EVNTSAPI] 
	  where EOM=@End_Month 
			AND CATHIT IN ('EN COVID19','EN Income Pitch','EN Employment Pitch'
	  ,'EN Consumer Misunderstanding','EN SCRA','EN Fraud Dispute'
	  ,'EN Balance Dispute','EN General Dispute','EN Attorney CS'
	  ,'EN Bankruptcy','EN DNC','EN Deceased','EN Wrong Number CS'
	  ,'EN Controller Consumer','EN Avoider Consumer','EN Pitcher Consumer'
	  ,'EN Promiser Consumer','EN Call Back Request','Em Care'
	  ,'EN Possible Spanish CS','Motive Payment','Motive Previous Call'
	  ,'Motive Mail','Spanish Request','CBR Mentions','IB Misc'
	  ,'Motive Digital Media','EN DCA Mention CS','EN Tax Language CX'
	  ,'EN Denied Info CX','EN CX Refusal Verification','EN CX Payment Information'
	  ,'EN CBR Removal CX','Compliments','Customer Compliments'
	  ,'EN AXP Creditor Name','EN AXP Balance')
	  AND CALLID IN (SELECT DISTINCT CALLID FROM dbo.#Results1)

	--extarcting the category group
	IF OBJECT_ID('tempdb.dbo.#Results2') IS NOT NULL 
	DROP TABLE dbo.#Results2
	SELECT DISTINCT [CATHIT]
		  ,[COMPNAME]
		  ,[CATHITCATG]
	INTO dbo.#Results2
	FROM [dbo].[CM_CXCATGRP]

	-- Calculating & Extarcting Unique Second Voice
	IF OBJECT_ID('tempdb.dbo.#Results_2') IS NOT NULL 
	DROP TABLE dbo.#Results_2
	SELECT CALLID
		  ,STTIME
	INTO dbo.#Results_2
	FROM (SELECT CALLID
		  ,STTIME
		  ,ROW_NUMBER() OVER (PARTITION BY CALLID 
		  ORDER BY STTIME DESC) AS RNK1
		  FROM dbo.#Results) a
	WHERE a.RNK1=1

	-- Calculating & Extarcting Unique Payment verification Category 
	IF OBJECT_ID('tempdb.dbo.#ResultsPV1') IS NOT NULL 
	DROP TABLE dbo.#ResultsPV1
	SELECT CALLID
		  ,STTIME
	INTO dbo.#ResultsPV1
	FROM (SELECT CALLID
		  ,STTIME
		  ,ROW_NUMBER() OVER (PARTITION BY CALLID 
		  ORDER BY STTIME DESC) AS RNK1
		  FROM dbo.#ResultsPV) a
	WHERE a.RNK1=1

	-- EXTRACTING OPENING time 
	IF OBJECT_ID('tempdb.dbo.#Results_3') IS NOT NULL 
	DROP TABLE dbo.#Results_3
	SELECT CALLID
		  ,STTIME
		  ,ROW_NUMBER() OVER (PARTITION BY CALLID 
			ORDER BY STTIME DESC) as RNK1
	INTO dbo.#Results_3 
	FROM dbo.#Results_1
	WHERE CATHIT IN ('EN AXP Creditor Name','EN AXP Balance')
		  AND STTIME<=180

	IF OBJECT_ID('tempdb.dbo.#Results_4') IS NOT NULL 
	DROP TABLE dbo.#Results_4
	SELECT CALLID
		  ,STTIME AS OPNCAL 
	INTO dbo.#Results_4
	FROM dbo.#Results_3 
	WHERE RNK1=1

	-- extarcting consumer words CALLS
	IF OBJECT_ID('tempdb.dbo.#Results_5') IS NOT NULL 
	DROP TABLE dbo.#Results_5
	SELECT CALLID
		  ,CATHIT
		  ,COMPNAME
		  ,CXLANGTIME
		  ,LANGFLG
	INTO dbo.#Results_5 
	FROM(
		 SELECT B.CALLID
			   ,B.CATHIT
			   ,B.COMPNAME
			   ,B.CXLANGTIME
			   ,B.LANGFLG
			   ,ROW_NUMBER() OVER(PARTITION BY B.callid,B.LANGFLG
				order by CXLANGTIME) rnk2
		 FROM
		 (	
		  SELECT A.CALLID
				,A.CATHIT
				,A.COMPNAME
				,A.STTIME AS CXLANGTIME
				,CASE WHEN B.STTIME>A.STTIME THEN 'Agent'
				  ELSE 'Supervisor' END AS LANGFLG
		  FROM dbo.#Results_1 A 
			LEFT JOIN dbo.#Results_2 B ON A.CALLID=b.CALLID
	WHERE A.CATHIT IN ('EN COVID19','EN Income Pitch','EN Employment Pitch'
	,'EN Consumer Misunderstanding','EN SCRA','EN Fraud Dispute','EN Balance Dispute'
	,'EN General Dispute','EN Attorney CS','EN Bankruptcy','EN DNC','EN Deceased'
	,'EN Wrong Number CS','EN Controller Consumer','EN Avoider Consumer'
	,'EN Pitcher Consumer','EN Promiser Consumer','EN Call Back Request','Em Care'
	,'EN Possible Spanish CS','Motive Payment','Motive Previous Call'
	,'Motive Mail','Spanish Request','CBR Mentions','IB Misc','Motive Digital Media'
	,'EN DCA Mention CS','EN Tax Language CX','EN Denied Info CX'
	,'EN CX Refusal Verification','EN CX Payment Information','EN CBR Removal CX'
	,'Compliments','Customer Compliments'))B) a
	WHERE a.rnk2=1

	-- extarcting consumer words CALLS given to agent
	IF OBJECT_ID('tempdb.dbo.#Results_5_1') IS NOT NULL 
	DROP TABLE dbo.#Results_5_1
	SELECT CALLID
		  ,CATHIT AS CXCATHITAGNT
		  ,COMPNAME AS CXCOMPNAMEAGNT
		  ,CXLANGTIME AS CXLANGTIMEAGNT
	INTO dbo.#Results_5_1
	FROM dbo.#Results_5
	WHERE LANGFLG='Agent'

	-- extarcting consumer words CALLS given to Supervisor
	IF OBJECT_ID('tempdb.dbo.#Results_5_2') IS NOT NULL 
	DROP TABLE dbo.#Results_5_2
	SELECT CALLID
		  ,CATHIT AS CXCATHITSUP
		  ,COMPNAME AS CXCOMPNAMESUP
		  ,CXLANGTIME AS CXLANGTIMESUP
	INTO dbo.#Results_5_2
	FROM dbo.#Results_5
	WHERE LANGFLG<>'Agent'

	--filetring Calls with completing the opening
	IF OBJECT_ID('tempdb.dbo.#Results_6') IS NOT NULL 
	DROP TABLE dbo.#Results_6
	SELECT A.EOM
		  ,A.AGNTID
		  ,A.CALLID
		  ,A.CALLDT
		  ,A.RGSSEID
		  ,A.DIS1
		  ,A.RECDP
		  ,A.PHNUMB
		  ,A.CLNTID
		  ,A.SILT
		  ,A.CLDUR
		  ,A.CATCNT
		  ,A.DIS1CAT 
		  ,B.OPNCAL
		  ,1 AS CALLCOUNT
		  ,CASE WHEN C.CALLID IS NULL THEN 0 ELSE 1 END SCNDVC
		  ,CASE WHEN C.CALLID IS NULL THEN 0 ELSE C.STTIME END SCNDVCTM
		  ,CASE WHEN G.CALLID IS NULL THEN 0 ELSE 1 END PVER
		  ,CASE WHEN G.CALLID IS NULL THEN 0 ELSE G.STTIME END PVERTM
		  ,CASE WHEN C.CALLID IS NULL THEN 'Not Applicable' 
				WHEN D.CALLID is not NULL THEN 
					 D.CXCATHITAGNT ELSE 'No Content' END AS CXCATHITAGNT
		  ,CASE WHEN C.CALLID IS NULL THEN 'Not Applicable' 
				WHEN D.CXCOMPNAMEAGNT is not NULL THEN 
					 D.CXCOMPNAMEAGNT ELSE 'No Language'END AS CXCOMPNAMEAGNT
		  ,CASE WHEN D.CXLANGTIMEAGNT is not NULL THEN 
					 D.CXLANGTIMEAGNT ELSE 0 END AS CXLANGTIMEAGNT
		  ,CASE WHEN C.CALLID IS NULL THEN 'Not Applicable' 
				WHEN E.CALLID is not NULL THEN 
					 E.CXCATHITSUP ELSE 'No Content' END AS CXCATHITSUP
		  ,CASE WHEN C.CALLID IS NULL THEN 'Not Applicable' 
				WHEN E.CXCOMPNAMESUP is not NULL THEN 
					 E.CXCOMPNAMESUP ELSE 'No Language'END AS CXCOMPNAMESUP
		  ,CASE WHEN E.CXLANGTIMESUP is not NULL THEN 
					 E.CXLANGTIMESUP ELSE 0 END AS CXLANGTIMESUP
		  ,CASE WHEN D.CXCATHITAGNT='Not Applicable' THEN 'Not Applicable'
				WHEN D.CXCATHITAGNT<>'No Content' THEN 
					 F.CATHITCATG ELSE 'No Content' END AS CXLANGAGNTGRP
	INTO dbo.#Results_6
	FROM dbo.#Results1 A Left Join
		 dbo.#Results_4 B on A.CALLID=b.CALLID
		 LEFT JOIN dbo.#Results_2 C on b.CALLID=C.CALLID
		 LEFT JOIN #ResultsPV1 G on A.CALLID=G.CALLID
		 LEFT JOIN dbo.#Results_5_1 D on C.CALLID=d.CALLID
		 LEFT JOIN dbo.#Results_5_2 E on C.CALLID=E.CALLID
		 LEFT JOIN dbo.#Results2 F on D.CXCATHITAGNT=F.CATHIT AND D.CXCOMPNAMEAGNT=F.COMPNAME
	WHERE B.CALLID IS NOT NULL

	--filetring Calls with completing the opening
	IF OBJECT_ID('tempdb.dbo.#Results_7') IS NOT NULL 
	DROP TABLE dbo.#Results_7
	SELECT A.EOM
		  ,A.AGNTID
		  ,A.CALLID
		  ,A.CALLDT
		  ,A.RGSSEID
		  ,A.DIS1
		  ,A.RECDP
		  ,A.PHNUMB
		  ,A.CLNTID
		  ,A.SILT
		  ,A.CLDUR
		  ,A.CATCNT
		  ,A.DIS1CAT 
		  ,A.OPNCAL
		  ,A.CALLCOUNT
		  ,A.SCNDVC
		  ,A.SCNDVCTM
		  ,A.PVER
		  ,A.PVERTM
		  ,A.CXCATHITAGNT
		  ,A.CXCOMPNAMEAGNT
		  ,A.CXLANGTIMEAGNT
		  ,A.CXCATHITSUP
		  ,A.CXCOMPNAMESUP
		  ,A.CXLANGTIMESUP
		  ,A.CXLANGAGNTGRP
		  ,CASE WHEN A.CXCATHITSUP='Not Applicable' THEN 'Not Applicable'
				WHEN A.CXCATHITSUP<>'No Content' THEN 
					 B.CATHITCATG ELSE 'No Content' END AS CXLANGSUPGRP
	INTO dbo.#Results_7 
	FROM dbo.#Results_6 A
		LEFT JOIN dbo.#Results2 B on A.CXCATHITSUP=B.CATHIT 
									 AND A.CXCOMPNAMESUP=B.COMPNAME

	--filetring Calls with completing the opening
	IF OBJECT_ID('tempdb.dbo.#Results_8') IS NOT NULL 
	DROP TABLE dbo.#Results_8
	SELECT A.EOM
		  ,A.AGNTID
		  ,A.CALLID
		  ,A.CALLDT
		  ,A.RGSSEID
		  ,A.DIS1
		  ,A.RECDP
		  ,A.PHNUMB
		  ,A.CLNTID
		  ,A.SILT
		  ,A.CLDUR
		  ,A.CATCNT
		  ,A.DIS1CAT 
		  ,A.OPNCAL
		  ,A.CALLCOUNT
		  ,A.SCNDVC
		  ,A.SCNDVCTM
		  ,A.PVER
		  ,A.PVERTM
		  ,A.CXCATHITAGNT
		  ,A.CXCOMPNAMEAGNT
		  ,A.CXLANGTIMEAGNT
		  ,A.CXCATHITSUP
		  ,A.CXCOMPNAMESUP
		  ,A.CXLANGTIMESUP
		  ,A.CXLANGAGNTGRP
		  ,A.CXLANGSUPGRP
		  ,A.CLDUR-A.SILT AS ACTTALKTIME
		  ,CASE WHEN A.SCNDVC>0 THEN A.CLDUR-A.SCNDVCTM ELSE 0 END AS SUPTIME
		  ,CASE WHEN A.SCNDVC>0 THEN A.SCNDVCTM ELSE A.CLDUR END AS  AGNTTIME
		  ,CASE WHEN A.DIS1CAT='PTP' THEN 1 ELSE 0 END AS PTP_Flags
		  ,CASE WHEN A.SCNDVC>0 THEN 'Yes' ELSE 'No' END AS SCNDVC_Flags
		  ,CASE WHEN A.CXLANGAGNTGRP='No Content' AND A.CXLANGSUPGRP='Not Applicable' THEN 0
				WHEN A.CXLANGAGNTGRP=A.CXLANGSUPGRP THEN 0 ELSE 1 END BEHA_Change_Flag
	 INTO dbo.#Results_8
	 FROM dbo.#Results_7 A


	 DELETE FROM Speech_Analytics.dbo.CM_Rpt_ATTSVDC
	 WHERE EOM=@End_Month


	 INSERT INTO Speech_Analytics.dbo.CM_Rpt_ATTSVDC
	 SELECT  EOM
			,AGNTID
			,CALLID
			,CALLDT
			,RGSSEID
			,DIS1
			,RECDP
			,PHNUMB
			,CLNTID
			,SILT
			,CLDUR
			,CATCNT
			,DIS1CAT
			,OPNCAL
			,CALLCOUNT
			,SCNDVC
			,SCNDVCTM
			,PVER
			,PVERTM
			,CXCATHITAGNT
			,CXCOMPNAMEAGNT
			,CXLANGTIMEAGNT
			,CXCATHITSUP
			,CXCOMPNAMESUP
			,CXLANGTIMESUP
			,CXLANGAGNTGRP
			,CXLANGSUPGRP
			,ACTTALKTIME
			,SUPTIME
			,AGNTTIME
			,PTP_Flags
			,SCNDVC_Flags
			,BEHA_Change_Flag
	FROM #Results_8


	DECLARE @tab char(1) = CHAR(9)

	DECLARE @totcntmm VARCHAR (MAX);
	SELECT @totcntmm=CAST(COUNT(CALLID) AS varchar) FROM dbo.#Results_8
	PRINT @totcntmm

	DECLARE @body1 VARCHAR (MAX); 
			SET @body1 = 'Hi All,
		
	Daily ATT Second Voice SQL code executed for '+convert(varchar,@End_Month,102)+'

	Total count of rows added are '+ (@totcntmm) +'. 

	Regards,
	Business Analytics';
			print @body1
	DECLARE @subject1 VARCHAR (MAX); 
			SET @subject1 = 'Daily ATT Second Voice SQL code executed for ' + convert(varchar,@End_Month,102);
			print @subject1
		--send email
		if (SELECT count(CALLID) FROM dbo.#Results_8)>0
			EXEC msdb.dbo.sp_send_dbmail
			@profile_name = 'DW Mail',--@@SERVERNAME, --'DFW2-BISQL-001',
			@from_address ='Reports SpeechAnalytics <reports.speechanalytics@radiusgs.com>',
			@recipients = 'dw@radiusgs.com;business.analytics@radiusgs.com',
			@copy_recipients='Pulkit.Jain@radiusgs.com;',
			@subject = @subject1,
			@body = @body1;
END