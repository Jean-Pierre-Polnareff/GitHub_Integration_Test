

ALTER PROCEDURE usp_63_Rpt_SMS_IDL_Click 
AS 
BEGIN 
	--Setting a maxdate for automatic run
	DECLARE @maxdate DATETIME
	SELECT @maxdate=CAST(max(call_date) as date) 
			from DW_MSTR_DM.dbo.RadiusCall rc WITH (NOLOCK) 
			where service_id = 152009
	PRINT @maxdate

	DECLARE @mindate DATETIME
	SET @mindate=CAST(DATEADD(DAY,1,EOMONTH(@maxdate,-1)) as date) 
	PRINT @mindate

	IF OBJECT_ID('tempdb.dbo.#IDLRcDataRaw') IS NOT NULL 
	DROP TABLE dbo.#IDLRcDataRaw
	SELECT a.[Call_Date] AS RepExeDate
		  ,a.[Service_ID]
		  ,a.[Service_Name]
		  ,a.Livevox_Result
		  ,a.Account_Number
		  ,a.Session_Id
		  ,a.Platform_Id
		  ,CAST((a.First_Name+' '+ a.Last_Name) as VARCHAR(255)) Full_Name
		  ,a.Call_Connect_Time_CT
	INTO dbo.#IDLRcDataRaw 
	FROM DW_MSTR_DM.dbo.RadiusCall a WITH (NOLOCK) 
	where service_id = 152009 
		  AND Call_Date between @mindate and @maxdate
		  AND a.Account_Number not in (1028)

	IF OBJECT_ID('tempdb.dbo.#IDLRcDataRaw_N') IS NOT NULL 
	DROP TABLE dbo.#IDLRcDataRaw_N
	SELECT RepExeDate
		  ,[Service_ID]
		  ,[Service_Name]
		  ,Livevox_Result
		  ,Account_Number
		  ,Session_Id
		  ,Platform_Id
		  ,Full_Name
		  ,Call_Connect_Time_CT
		  ,LAG(Livevox_Result,1,0) OVER(PARTITION BY Account_Number 
			ORDER BY Call_Connect_Time_CT) RNK 
	INTo dbo.#IDLRcDataRaw_N
	FROM dbo.#IDLRcDataRaw 
	ORDER BY RepExeDate

	IF OBJECT_ID('tempdb.dbo.#IDLRcDataRaw_N1') IS NOT NULL 
	DROP TABLE dbo.#IDLRcDataRaw_N1
	SELECT RepExeDate
		  ,[Service_ID]
		  ,[Service_Name]
		  ,Livevox_Result
		  ,Account_Number
		  ,Session_Id
		  ,Platform_Id
		  ,Full_Name
		  ,Call_Connect_Time_CT
		  ,CASE WHEN RNK in ('Consumer responded Stop to text',
			'Consumer responded Help to text before Authentication') 
			and Livevox_Result IN ('SMS MO Received','SMS MT Delivered'
			,'SMS MT Failed','Operator Transfer') THEN 1 ELSE 0 END AS chk1 
	INTO dbo.#IDLRcDataRaw_N1
	FROM dbo.#IDLRcDataRaw_N
	ORDER BY RepExeDate,Account_Number,Call_Connect_Time_CT

	IF OBJECT_ID('tempdb.dbo.#IDLRcDataRaw_N2') IS NOT NULL 
	DROP TABLE dbo.#IDLRcDataRaw_N2
	SELECT t1.RepExeDate
		  ,t1.[Service_ID]
		  ,t1.[Service_Name]
		  ,t1.Livevox_Result
		  ,t1.Account_Number
		  ,t1.Session_Id
		  ,COALESCE(t1.Platform_Id, t2.LastPlatform_Id) AS Platform_Id
		  ,t1.Full_Name
		  ,t1.Call_Connect_Time_CT
	INTO dbo.#IDLRcDataRaw_N2
	FROM dbo.#IDLRcDataRaw_N1 t1
	OUTER APPLY (
		SELECT TOP 1 t2.Platform_Id AS LastPlatform_Id
		FROM dbo.#IDLRcDataRaw_N1 t2
		WHERE t2.Account_Number = t1.Account_Number
		  AND t2.Full_Name=t1.Full_Name
		  AND t2.Platform_Id IS NOT NULL
		ORDER BY t2.RepExeDate 
	) t2
	WHERE t1.chk1=0
	ORDER BY t1.Call_Connect_Time_CT

	IF OBJECT_ID('tempdb.dbo.#IDLRcDataRaw_N4') IS NOT NULL 
	DROP TABLE dbo.#IDLRcDataRaw_N4
	SELECT a.[Call_Date] AS RepExeDate
		  ,a.[Service_ID]
		  ,a.[Service_Name]
		  ,a.Livevox_Result
		  ,a.Account_Number
		  ,a.Session_Id
		  ,a.Platform_Id
		  ,CAST((a.First_Name+' '+ a.Last_Name) as VARCHAR(255)) Full_Name
		  ,a.Call_Connect_Time_CT
	INTO dbo.#IDLRcDataRaw_N4 
	FROM DW_MSTR_DM.dbo.RadiusCall a WITH (NOLOCK) 
	where service_id = 152009 
		 AND Account_Number in (SELECT DISTINCT Account_Number FROM #IDLRcDataRaw_N2
	WHERE Platform_Id IS NULL)

	IF OBJECT_ID('tempdb.dbo.#IDLRcDataRaw_N5') IS NOT NULL 
	DROP TABLE dbo.#IDLRcDataRaw_N5
	SELECT t1.RepExeDate
		  ,t1.[Service_ID]
		  ,t1.[Service_Name]
		  ,t1.Livevox_Result
		  ,t1.Account_Number
		  ,t1.Session_Id
		  ,COALESCE(t1.Platform_Id, t2.LastPlatform_Id) AS Platform_Id
		  ,t1.Full_Name
		  ,t1.Call_Connect_Time_CT
	INTO dbo.#IDLRcDataRaw_N5
	FROM dbo.#IDLRcDataRaw_N4 t1
	OUTER APPLY (
		SELECT TOP 1 t2.Platform_Id AS LastPlatform_Id
		FROM dbo.#IDLRcDataRaw_N4 t2
		WHERE t2.Account_Number = t1.Account_Number
		  AND t2.Full_Name=t1.Full_Name
		  AND t2.Platform_Id IS NOT NULL
		ORDER BY t2.RepExeDate 
	) t2
	ORDER BY t1.Call_Connect_Time_CT

	IF OBJECT_ID('tempdb.dbo.#IDLRcDataRaw_N3') IS NOT NULL 
	DROP TABLE dbo.#IDLRcDataRaw_N3
	SELECT RepExeDate
		  ,[Service_ID]
		  ,[Service_Name]
		  ,Livevox_Result
		  ,Account_Number
		  ,Session_Id
		  ,Platform_Id
		  ,Full_Name
		  ,Call_Connect_Time_CT 
	INTO dbo.#IDLRcDataRaw_N3
	FROM
	(
	SELECT RepExeDate
		  ,[Service_ID]
		  ,[Service_Name]
		  ,Livevox_Result
		  ,Account_Number
		  ,Session_Id
		  ,Platform_Id
		  ,Full_Name
		  ,Call_Connect_Time_CT 	  
	FROM dbo.#IDLRcDataRaw_N2
	WHERE Platform_Id IS NOT NULL
	UNION
	SELECT t1.RepExeDate
		  ,t1.[Service_ID]
		  ,t1.[Service_Name]
		  ,t1.Livevox_Result
		  ,t1.Account_Number
		  ,t1.Session_Id
		  ,t2.Platform_Id
		  ,t1.Full_Name
		  ,t1.Call_Connect_Time_CT 
	FROM dbo.#IDLRcDataRaw_N2 t1
		LEFT JOIN  dbo.#IDLRcDataRaw_N5 t2
			ON t1.Session_Id=t2.Session_Id
	WHERE t1.Platform_Id IS NULL
	) a

	IF OBJECT_ID('tempdb.dbo.#IDLRcDataRaw_N6') IS NOT NULL 
	DROP TABLE dbo.#IDLRcDataRaw_N6
	SELECT RepExeDate
		  ,[Service_ID]
		  ,[Service_Name]
		  ,Livevox_Result
		  ,CAST((REPLACE(Account_Number,'Med','')*1) as bigint) Account_Number
		  ,Session_Id
		  ,Platform_Id
		  ,Full_Name
		  ,Call_Connect_Time_CT 
	INTO dbo.#IDLRcDataRaw_N6
	FROM #IDLRcDataRaw_N3

	IF OBJECT_ID('tempdb.dbo.#IDLRcDataRaw_1') IS NOT NULL 
	DROP TABLE dbo.#IDLRcDataRaw_1
	SELECT a.RepExeDate
		  ,a.[Service_ID]
		  ,a.[Service_Name]
		  ,a.Livevox_Result
		  ,a.Session_Id
		  ,a.Account_Number
		  ,a.Platform_Id
		  ,a.Full_Name
		  ,a.Call_Connect_Time_CT 
		  ,dcu.Clientid
	 INTO dbo.#IDLRcDataRaw_1
	 FROM #IDLRcDataRaw_N3 a
	 LEFT JOIN  [DW_MSTR_DM].[dbo].[DimCustomer] dcu with (nolock)
		on a.Account_Number=dcu.[CustomerId]
	WHERE a.Platform_Id='ARTIVA_3' 
		and dcu.KeySourceSystem=2

	IF OBJECT_ID('tempdb.dbo.#IDLRcDataRaw_2') IS NOT NULL 
	DROP TABLE dbo.#IDLRcDataRaw_2
	SELECT a.RepExeDate
		  ,a.[Service_ID]
		  ,a.[Service_Name]
		  ,a.Livevox_Result
		  ,a.Session_Id
		  ,a.Account_Number
		  ,a.Platform_Id
		  ,a.Full_Name
		  ,a.Call_Connect_Time_CT 
		  ,dcu.Clientid
	 INTO dbo.#IDLRcDataRaw_2
	 FROM #IDLRcDataRaw_N3 a
	 LEFT JOIN  [DW_MSTR_DM].[dbo].[DimCustomer] dcu with (nolock)
		on a.Account_Number=dcu.[CustomerId]
	WHERE a.Platform_Id<>'ARTIVA_3' 
		and dcu.KeySourceSystem=1

	IF OBJECT_ID('tempdb.dbo.#IDLRcDataRaw_3') IS NOT NULL 
	DROP TABLE dbo.#IDLRcDataRaw_3
	SELECT RepExeDate
		  ,[Service_ID]
		  ,[Service_Name]
		  ,Livevox_Result
		  ,Account_Number
		  ,Platform_Id
		  ,Clientid
		  ,Session_Id
		  ,Full_Name
		  ,Call_Connect_Time_CT 
	INTO dbo.#IDLRcDataRaw_3
	FROM
	(
		SELECT RepExeDate
		  ,[Service_ID]
		  ,[Service_Name]
		  ,Livevox_Result
		  ,Account_Number
		  ,Platform_Id
		  ,Clientid
		  ,Session_Id
		  ,Full_Name
		  ,Call_Connect_Time_CT 
		FROM dbo.#IDLRcDataRaw_1
		UNION
		SELECT RepExeDate
		  ,[Service_ID]
		  ,[Service_Name]
		  ,Livevox_Result
		  ,Account_Number
		  ,Platform_Id
		  ,Clientid
		  ,Session_Id
		  ,Full_Name
		  ,Call_Connect_Time_CT 
	FROM dbo.#IDLRcDataRaw_2) a

	IF OBJECT_ID('tempdb.dbo.#IDLRcDataRaw_4') IS NOT NULL 
	DROP TABLE dbo.#IDLRcDataRaw_4
	SELECT a.RepExeDate
		  ,a.[Service_ID]
		  ,a.[Service_Name]
		  ,a.Livevox_Result
		  ,a.Account_Number
		  ,a.Platform_Id
		  ,a.Full_Name
		  ,a.Call_Connect_Time_CT 
		  ,b.Clientid
		  ,a.Session_Id
	INTO dbo.#IDLRcDataRaw_4
	FROM dbo.#IDLRcDataRaw_N6 a
		LEFT JOIN dbo.#IDLRcDataRaw_3 b on a.Session_Id=b.Session_Id
		and a.RepExeDate=b.RepExeDate and a.Account_Number=b.Account_Number

	IF OBJECT_ID('tempdb.dbo.#IDLClickDataRaw') IS NOT NULL 
	DROP TABLE dbo.#IDLClickDataRaw
	SELECT *
	INTO dbo.#IDLClickDataRaw 
	FROM [DW_MSTR_DM].[dbo].[FactIDNSMSClick]
	  WHERE [SMSClickedDate] between @mindate and @maxdate

	IF OBJECT_ID('tempdb.dbo.#IDLClickDataRaw_1') IS NOT NULL 
	DROP TABLE dbo.#IDLClickDataRaw_1
	  SELECT a.RepExeDate
		  ,a.[Service_ID]
		  ,a.[Service_Name]
		  ,a.Livevox_Result
		  ,a.Account_Number
		  ,a.Platform_Id
		  ,a.Clientid
		  ,a.Session_Id
		  ,b.LetterID
	  INTO dbo.#IDLClickDataRaw_1
	  FROM dbo.#IDLRcDataRaw_4 a
			LEFT JOIN dbo.#IDLClickDataRaw b 
				on a.Account_Number=b.AccountNumber
					AND a.ClientId=b.ClientID
	  WHERE b.SMSClickedDate=a.RepExeDate
	  order by RepExeDate

	IF OBJECT_ID('tempdb.dbo.#TotClicks') IS NOT NULL 
	DROP TABLE dbo.#TotClicks
	SELECT RepExeDate
		  ,Account_Number 
		  ,count([Service_Name]) as TotalClicks
	INTO dbo.#TotClicks
	FROM dbo.#IDLClickDataRaw_1
	GROUP BY RepExeDate
			,Account_Number

	IF OBJECT_ID('tempdb.dbo.#UniqClicks') IS NOT NULL 
	DROP TABLE dbo.#UniqClicks
	SELECT RepExeDate
		  ,Account_Number
		  ,count([Service_Name]) as UniqueClicks
	INTO dbo.#UniqClicks
	FROM (SELECT DISTINCT RepExeDate
						 ,[Service_Name]
						 ,LetterID
						 , Account_Number
		 FROM dbo.#IDLClickDataRaw_1) a
	GROUP BY RepExeDate
			,Account_Number

	IF OBJECT_ID('tempdb.dbo.#IDLRcData_1') IS NOT NULL 
	DROP TABLE dbo.#IDLRcData_1
	SELECT a.RepExeDate
		  ,a.[Service_ID]
		  ,a.[Service_Name]
		  ,a.Livevox_Result
		  ,a.Account_Number
		  ,a.Platform_Id
		  ,a.Full_Name
		  ,a.Call_Connect_Time_CT 
		  ,a.Clientid
		  ,a.Session_Id
		  ,ISNULL(b.TotalClicks,0)TotalClicks
		  ,ISNULL(c.UniqueClicks,0)UniqueClicks
		  ,CASE WHEN a.Livevox_Result in('Consumer responded Help to text before Authentication'
			,'Consumer responded Stop to text','SMS MO Received','SMS MT Delivered'
			,'SMS MT Failed','Operator Transfer') THEN 1 ELSE 0 END as Total_DIALED
		 ,CASE WHEN Livevox_Result in('SMS MT Delivered','Operator Transfer') 
			THEN 1 ELSE 0 END as Total_Connected
		 ,CASE WHEN Livevox_Result in('Consumer responded Stop to text',
			'Consumer responded Help to text before Authentication') 
			THEN 1 ELSE 0 END as Total_Stop
	INTO dbo.#IDLRcData_1
	FROM dbo.#IDLRcDataRaw_4 a
		LEFT JOIN dbo.#TotClicks b
			on a.RepExeDate=b.RepExeDate
				AND a.Account_Number=b.Account_Number
		LEFT JOIN dbo.#UniqClicks c
			on a.RepExeDate=c.RepExeDate
				AND a.Account_Number=c.Account_Number

	DELETE FROM Client_Analytics.dbo.RPT_Daily_IDL_Click
	WHERE RepExeDate between @mindate and @maxdate

	INSERT INTO Client_Analytics.dbo.RPT_Daily_IDL_Click
	(	RepExeDate
		  ,Service_ID
		  ,ServiceName
		  ,Livevox_Result
		  ,Account_Number
		  ,Platform_Id
		  ,Full_Name
		  ,Call_Connect_Time_CT
		  ,Clientid
		  ,Session_Id
		  ,TotalClicks
		  ,UniqueClicks
		  ,Total_DIALED
		  ,Total_Connected
		  ,Total_Stop
	)
	SELECT RepExeDate
		  ,Service_ID
		  ,[Service_Name] as ServiceName
		  ,Livevox_Result
		  ,Account_Number
		  ,Platform_Id
		  ,Full_Name
		  ,Call_Connect_Time_CT
		  ,Clientid
		  ,Session_Id
		  ,TotalClicks
		  ,UniqueClicks
		  ,Total_DIALED
		  ,Total_Connected
		  ,Total_Stop
	 FROM dbo.#IDLRcData_1
	 ORDER BY RepExeDate

	DECLARE @tab char(1) = CHAR(9)

	DECLARE @totcntmm VARCHAR (MAX);
	SELECT @totcntmm=CAST(COUNT(RepExeDate) AS varchar) FROM dbo.#IDLRcData_1
	PRINT @totcntmm

	DECLARE @body1 VARCHAR (MAX); 
			SET @body1 = 'Hi All,
		
	Daily IDL Click Summary SQL code executed for '+convert(varchar,@maxdate,102)+'

	Total count of rows added are '+ (@totcntmm) +'. 

	Regards,
	Business Analytics';
			print @body1
	DECLARE @subject1 VARCHAR (MAX); 
			SET @subject1 = 'Daily IDL Click SQL code executed for ' + convert(varchar,@maxdate,102);
			print @subject1
		--send email
		if (SELECT count(RepExeDate) FROM dbo.#IDLRcData_1)>0
			EXEC msdb.dbo.sp_send_dbmail
			@profile_name = 'DW Mail',--@@SERVERNAME, --'DFW2-BISQL-001',
			@from_address ='Reports SpeechAnalytics <reports.speechanalytics@radiusgs.com>',
			@recipients = 'business.analytics@radiusgs.com;dw@radiusgs.com',
			@copy_recipients='Pulkit.Jain@radiusgs.com;',
			@subject = @subject1,
			@body = @body1; 
END 
GO 

