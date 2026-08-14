USE [CLIENT_ANALYTICS]
GO
/****** Object:  StoredProcedure [dbo].[usp_61_Rpt_Pendricks_Kpi_Daily]    Script Date: 9/26/2024 6:50:30 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO


ALTER PROCEDURE [dbo].[usp_61_Rpt_Pendricks_Kpi_Daily]
	
AS
BEGIN
	SET NOCOUNT ON;

	 --Setting a eomdate for automatic run
	DECLARE @End_Month DATETIME
	SET @End_Month=EOMONTH(CAST(DATEADD(DAY,-3,GETDATE()) AS DATE ),0)
	PRINT @End_Month

	--Setting a start of month for automatic run
	DECLARE @Start_Month DATETIME
	SET @Start_Month=CAST(DATEADD(DAY,1,EOMONTH(@End_Month,-1)) AS DATE )
	PRINT @Start_Month

	Declare @End_Month_i bigint 
	SET @End_Month_i=(YEAR(@End_Month)*10000)+(MONTH(@End_Month)*100)+(Day(@End_Month))
	PRINT @End_Month_i

	Declare @Start_Month_i bigint
	SET @Start_Month_i=(YEAR(@Start_Month)*10000)+(MONTH(@Start_Month)*100)+(Day(@Start_Month))
	PRINT @Start_Month_i

	IF OBJECT_ID('tempdb.dbo.#PenClientFnl') IS NOT NULL 
	DROP TABLE tempdb.dbo.#PenClientFnl;
	IF OBJECT_ID('tempdb.dbo.#PenClientData') IS NOT NULL 
	DROP TABLE tempdb.dbo.#PenClientData;
	CREATE TABLE #PenClientData (
		ServiceID INT,
		ServiceName NVARCHAR(100)
	);

	-- Insert some sample data into the temporary table
	INSERT INTO #PenClientData (ServiceID, ServiceName)
	SELECT ROW_NUMBER() OVER (PARTITION BY ServiceName ORDER BY ServiceName) ServiceID,
	ServiceName
	FROM 
	(
	SELECT DISTINCT ActualDistribution as ServiceName
	FROM  [CLIENT_ANALYTICS].[dbo].[DimPendrickServiceCrm]
	) a;

	-- Generate dates between @StartDate and @EndDate using a recursive CTE
	WITH DateRange AS (
		SELECT @Start_Month AS DateValue
		UNION ALL
		SELECT DATEADD(DAY, 1, DateValue)
		FROM DateRange
		WHERE DATEADD(DAY, 1, DateValue) <= @End_Month
	)
	-- Cross join the DateRange with the Clients
	SELECT 
		s.ServiceName,
		CAST(d.DateValue as date) DateValue
	INTO dbo.#PenClientFnl
	FROM 
		DateRange d
	CROSS JOIN 
		#PenClientData s
	OPTION (MAXRECURSION 0); -- Allow recursion to go beyond the default 100 levels

	IF OBJECT_ID('tempdb.dbo.#PenSerId') IS NOT NULL 
	DROP TABLE dbo.#PenSerId
	 SELECT a.[ServiceID]
		  ,a.[CampaignName]
		  ,a.ServiceType
		  ,CASE WHEN b.ActualDistribution IS NULL THEN 'Pendrick Prime' 
			ELSE b.ActualDistribution END AS ServiceName
	  INTO dbo.#PenSerId
	  FROM [CLIENT_ANALYTICS].[dbo].[DimServiceNames] a LEFT JOIN 
	  [CLIENT_ANALYTICS].[dbo].[DimPendrickServiceCrm] b 
		on a.[CampaignName]=b.[CampaignName]
	  WHERE a.[CampaignName] IN ('AST - Pendrick','DEC - Pendrick',
	  'LXA - Pendrick','MBJ - Pen Primes','MBJ - Pen WN',
	  'MUM - Pendrick','MUM - Pendrick 2','MUM - Pendrick 3',
	  'MUM - Pendrick 4','MUM - Pendrick 5','MUM - Pendrick 6',
	  'Nevis - Pendrick','LXA - Shumaker')
	  ORDER BY a.[CampaignName]

	  --Extracting Kpi data for above Service IDs
	IF OBJECT_ID('tempdb.dbo.#PenSerKpi') IS NOT NULL 
	DROP TABLE dbo.#PenSerKpi
	SELECT CAST(a.[DateStarted] as date) as DateValue
		  ,b.ServiceName
		  ,SUM(a.[CallsOffered])[CallsOffered]
		  ,SUM(a.[CallsAbandoned])[CallsAbandoned]
		  ,SUM(a.[CallsAnswered])[CallsAnswered]
		  ,SUM(a.[CallsAnsweredIn20s])[CallsAnsweredIn20s]
		  ,SUM(a.[IBRPCs])[IBRPCs]
		  ,SUM(a.[IBPaymentPTP])[IBPaymentPTP]
		  ,SUM(a.[PaymentPTP])[PaymentPTP]
		  ,SUM(a.[PaymentPTPAmt])[PaymentPTPAmt]
		  ,SUM(a.[OBCallsAbandoned])[OBCallsAbandoned]
		  ,SUM(a.[OBAttemptedOperatorTransfers])[OBAttemptedOperatorTransfers]
		  ,SUM(a.[OBCallsAnswered])[OBCallsAnswered]
		  ,SUM(a.[OBRPCs])[OBRPCs]
		  ,SUM(a.[OBPaymentPTP])[OBPaymentPTP]
		  ,SUM(a.[OBPaymentAmt])[OBPaymentAmt]
		  ,SUM(a.[SystemHrs])[SystemHrs]
		  ,SUM(a.[ReadyHrs])[ReadyHrs]
		  ,SUM(a.[WrapupHrs])[WrapupHrs]
		  ,SUM(a.[AccountsWorked])[AccountsWorked]
	  INTO dbo.#PenSerKpi
	  FROM [CLIENT_ANALYTICS].[dbo].[FactPendrcksKPI] a with (nolock)
		LEFT JOIN dbo.#PenSerId b on a.ServiceID=b.ServiceID
	  WHERE a.[ServiceID] in (SELECT DISTINCT [ServiceID] FROM dbo.#PenSerId)
		and cast(a.[DateStarted] as date) between @Start_Month and @End_Month
	  GROUP BY a.[DateStarted]
			  ,b.ServiceName

	--Extracting client ids for pendricks
	IF OBJECT_ID('tempdb.dbo.#PenSerClId') IS NOT NULL 
	DROP TABLE dbo.#PenSerClId
	SELECT a.[ClientId]
		  ,a.ActualDistribution as  ServiceName
		  ,b.[KeyClient]
	 INTO dbo.#PenSerClId
	  FROM [CLIENT_ANALYTICS].[dbo].[DimPendrickServiceCrm] a 
	  LEFT JOIN [DW_MSTR_DM].[dbo].[DimClient] b
		on a.[ClientId]=b.[ClientId]

	--Letters Data
	 IF OBJECT_ID('tempdb.dbo.#PenSerletters') IS NOT NULL 
	DROP TABLE dbo.#PenSerletters
	SELECT b.ServiceName
		  ,CONVERT(DATE, 
				   CAST(LEFT(a.KeyDate_MailDate, 4) AS CHAR(4)) + '-' +
				   CAST(SUBSTRING(CAST(a.KeyDate_MailDate AS CHAR(8)), 5, 2) AS CHAR(2)) + '-' +
				   CAST(RIGHT(a.KeyDate_MailDate, 2) AS CHAR(2)), 
				   120) AS DateValue
		  ,COUNT(a.KeyCustomer) AS LETTER_Count
	INTO dbo.#PenSerletters
	FROM [DW_MSTR_DM].[dbo].[FactCustomerLetter] a with (nolock)
		LEFT JOIN dbo.#PenSerClId b on a.KeyClient=b.KeyClient
	WHERE a.KeyDate_MailDate between @Start_Month_i and @End_Month_i
	AND a.KeyClient IN (SELECT DISTINCT [KeyClient] FROm dbo.#PenSerClId)
	group by b.ServiceName
		  ,a.KeyDate_MailDate

	IF OBJECT_ID('tempdb.dbo.#PenSerletters_1') IS NOT NULL 
	DROP TABLE dbo.#PenSerletters_1
	SELECT ServiceName
		  ,DateValue
		  ,SUM(LETTER_Count) as LETTER_Count 
	INTO dbo.#PenSerletters_1
	FROM dbo.#PenSerletters
	group by ServiceName
			,DateValue

	--Emails Data
	IF OBJECT_ID('tempdb.dbo.#PenSeremails') IS NOT NULL 
	DROP TABLE dbo.#PenSeremails
	SELECT b.ServiceName
		  ,CAST(a.EventDate AS date) DateValue
		  ,COUNT(a.KeyCustomer) as Email_Count 
	INTO dbo.#PenSeremails
	FROM [DW_MSTR_DM].[dbo].[Radius_EmailReportData] a with (nolock)
		LEFT JOIN dbo.#PenSerClId b on a.KeyClient=b.KeyClient
	WHERE CAST(a.EventDate as date) between @Start_Month and @End_Month
	AND a.KeyClient IN (SELECT DISTINCT [KeyClient] FROm dbo.#PenSerClId)
	AND a.EventValue like '%sent%'
	group by b.ServiceName
		  ,a.EventDate

	IF OBJECT_ID('tempdb.dbo.#PenSeremails_1') IS NOT NULL 
	DROP TABLE dbo.#PenSeremails_1
	SELECT ServiceName
		  ,DateValue
		  ,SUM(Email_Count) as Email_Count 
	INTO dbo.#PenSeremails_1
	FROM dbo.#PenSeremails
	group by ServiceName
			,DateValue


	IF OBJECT_ID('tempdb.dbo.#PenSerCancelacc') IS NOT NULL 
	DROP TABLE dbo.#PenSerCancelacc
	SELECT b.ServiceName
		  ,CAST(a.CancelDate as date) DateValue
		  ,COUNT(a.[ListDate]) AS Cancel_Accounts
	INTO dbo.#PenSerCancelacc
	FROM [DW_MSTR_DM].[dbo].[DimCustomer] a with (nolock)
		LEFT JOIN dbo.#PenSerClId b on a.KeyClient=b.KeyClient
	WHERE CAST(a.CancelDate as date) between @Start_Month and @End_Month
		AND b.ServiceName IS NOT NULL
	group by b.ServiceName
			,a.CancelDate

	IF OBJECT_ID('tempdb.dbo.#PenSerCancelacc_1') IS NOT NULL 
	DROP TABLE dbo.#PenSerCancelacc_1
	SELECT ServiceName
		  ,DateValue
		  ,SUM(Cancel_Accounts) OVER (PARTITION BY ServiceName ORDER BY DateValue) 
			Cumcancel_Accounts
	INTO dbo.#PenSerCancelacc_1
	FROM dbo.#PenSerCancelacc
	order by ServiceName
			,DateValue

	IF OBJECT_ID('tempdb.dbo.#PenSernewacc') IS NOT NULL 
	DROP TABLE dbo.#PenSernewacc
	SELECT b.ServiceName
		  ,CAST(a.[ListDate] as date) DateValue
		  ,COUNT([ListDate]) AS New_Accounts
	INTO dbo.#PenSernewacc
	FROM [DW_MSTR_DM].[dbo].[DimCustomer] a with (nolock)
		LEFT JOIN dbo.#PenSerClId b on a.KeyClient=b.KeyClient
	WHERE CAST(a.[ListDate] as date) between @Start_Month and @End_Month
		AND b.ServiceName IS NOT NULL
	group by b.ServiceName
			,a.[ListDate]

	IF OBJECT_ID('tempdb.dbo.#PenSerallacc') IS NOT NULL 
	DROP TABLE dbo.#PenSerallacc
	SELECT b.ServiceName
		  ,COUNT([ListDate]) AS All_Accounts
	INTO dbo.#PenSerallacc
	FROM [DW_MSTR_DM].[dbo].[DimCustomer] a with (nolock)
		LEFT JOIN dbo.#PenSerClId b on a.KeyClient=b.KeyClient
	WHERE a.KeyClient IN (SELECT DISTINCT [KeyClient] FROm dbo.#PenSerClId)
		AND CAST(a.[ListDate] as date)<=@End_Month
		AND (CAST(a.CancelDate as date) IS NULL OR CAST(a.CancelDate as date)>=@Start_Month)
	group by b.ServiceName

	IF OBJECT_ID('tempdb.dbo.#PenSerSms') IS NOT NULL 
	DROP TABLE dbo.#PenSerSMS
	SELECT CAST(a.[DateStarted] as date) DateValue
		  ,b.[ServiceName]
		  ,SUM(a.[TotalCalls]) SMSSent
	INTO dbo.#PenSerSMS
	FROM [DW_MSTR_DM].[dbo].[FactLIveVoxSMSSummary] a 
		left join dbo.#PenSerId b on a.ServiceID=b.ServiceID
	WHERE CAST(a.[DateStarted] as date) between @Start_Month and @End_Month
		 AND b.[ServiceName] IS NOT NULL
		 AND b.ServiceType='OB-HTI'
	Group by a.[DateStarted]
			,b.[ServiceName]


	IF OBJECT_ID('tempdb.dbo.#PenSerKpi_1') IS NOT NULL 
	DROP TABLE dbo.#PenSerKpi_1
	SELECT a.ServiceName
		  ,a.DateValue
		  ,b.All_Accounts
		  ,d.Cumcancel_Accounts
		  ,e.Email_Count
		  ,f.LETTER_Count
		  ,g.CallsOffered
		  ,g.CallsAbandoned
		  ,g.CallsAnswered
		  ,g.CallsAnsweredIn20s
		  ,g.IBRPCs
		  ,g.IBPaymentPTP
		  ,g.PaymentPTP
		  ,g.PaymentPTPAmt
		  ,g.OBCallsAbandoned
		  ,g.OBAttemptedOperatorTransfers
		  ,g.OBCallsAnswered
		  ,g.OBRPCs
		  ,g.OBPaymentPTP
		  ,g.OBPaymentAmt
		  ,g.SystemHrs
		  ,g.ReadyHrs
		  ,g.WrapupHrs
		  ,g.AccountsWorked
		  ,c.SMSSent
	INTo dbo.#PenSerKpi_1
	FROM dbo.#PenClientFnl a
			LEFT JOIN dbo.#PenSerallacc b
				on a.ServiceName=b.ServiceName
			LEFT JOIN dbo.#PenSerCancelacc_1 d
				on a.ServiceName=d.ServiceName
					and a.DateValue=d.DateValue
			LEFT JOIN dbo.#PenSeremails_1 e
				on a.ServiceName=e.ServiceName
					and a.DateValue=e.DateValue	
			LEFT JOIN dbo.#PenSerletters_1 f
				on a.ServiceName=f.ServiceName
					and a.DateValue=f.DateValue	
			LEFT JOIN dbo.#PenSerKpi g
				on a.ServiceName=g.ServiceName
					and a.DateValue=g.DateValue
			LEFT JOIN  dbo.#PenSerSMS c
				on a.ServiceName=c.ServiceName
					and a.DateValue=c.DateValue
	 ORDER By a.ServiceName
		  ,a.DateValue

	IF OBJECT_ID('tempdb.dbo.#PenSerKpi_2') IS NOT NULL 
	DROP TABLE dbo.#PenSerKpi_2
	SELECT t1.ServiceName
		  ,t1.DateValue
		  ,t1.All_Accounts
		  ,COALESCE(t1.Cumcancel_Accounts, t2.LastCumcancel_Accounts) AS Cumcancel_Accounts
		  ,t1.All_Accounts-COALESCE(t1.Cumcancel_Accounts, 
			t2.LastCumcancel_Accounts) as Available_Accounts
		  ,ISNULL(t1.Email_Count,0)Email_Count
		  ,ISNULL(t1.LETTER_Count,0)LETTER_Count
		  ,ISNULL(t1.SMSSent,0) SMSSent
		  ,t1.CallsOffered
		  ,t1.CallsAbandoned
		  ,t1.CallsAnswered
		  ,t1.CallsAnsweredIn20s
		  ,t1.IBRPCs
		  ,t1.IBPaymentPTP
		  ,t1.PaymentPTP
		  ,t1.PaymentPTPAmt
		  ,t1.OBCallsAbandoned
		  ,t1.OBAttemptedOperatorTransfers
		  ,t1.OBCallsAnswered
		  ,t1.OBRPCs
		  ,t1.OBPaymentPTP
		  ,t1.OBPaymentAmt
		  ,t1.SystemHrs
		  ,t1.ReadyHrs
		  ,t1.WrapupHrs
		  ,t1.AccountsWorked
	INTo dbo.#PenSerKpi_2
	FROM 
		dbo.#PenSerKpi_1 t1
	OUTER APPLY (
		SELECT TOP 1 t2.Cumcancel_Accounts AS LastCumcancel_Accounts
		FROM dbo.#PenSerKpi_1 t2
		WHERE t2.ServiceName = t1.ServiceName
		  AND t2.DateValue < t1.DateValue
		  AND t2.Cumcancel_Accounts IS NOT NULL
		ORDER BY t2.DateValue DESC
	) t2
	ORDER BY 
		t1.ServiceName,
		t1.DateValue;

	IF OBJECT_ID('tempdb.dbo.#PenSerKpi_3') IS NOT NULL 
	DROP TABLE dbo.#PenSerKpi_3
	SELECT ServiceName
		  ,DateValue
		  ,All_Accounts
		  ,Cumcancel_Accounts
		  ,CASE WHEN LAG(Available_Accounts,1,0) OVER 
			(PARTITION BY ServiceName ORDER BY DateValue)=0 
			THEN All_Accounts ELSE LAG(Available_Accounts,1,0) 
			OVER (PARTITION BY ServiceName ORDER BY DateValue) END Available_Accounts
		  ,Email_Count
		  ,LETTER_Count
		  ,CallsOffered
		  ,CallsAbandoned
		  ,CallsAnswered
		  ,CallsAnsweredIn20s
		  ,IBRPCs
		  ,IBPaymentPTP
		  ,PaymentPTP
		  ,PaymentPTPAmt
		  ,OBCallsAbandoned
		  ,OBAttemptedOperatorTransfers
		  ,OBCallsAnswered
		  ,OBRPCs
		  ,OBPaymentPTP
		  ,OBPaymentAmt
		  ,SystemHrs
		  ,ReadyHrs
		  ,WrapupHrs
		  ,AccountsWorked
		  ,SMSSent
	INTO dbo.#PenSerKpi_3
	FROM dbo.#PenSerKpi_2

	DELETE FROM CLIENT_ANALYTICS.dbo.RPT_Pendricks_KPI_Daily
	WHERE DateValue Between @Start_Month and @End_Month

	INSERT INTO CLIENT_ANALYTICS.dbo.RPT_Pendricks_KPI_Daily
	SELECT ServiceName
		  ,DateValue
		  ,All_Accounts
		  ,Cumcancel_Accounts
		  ,Available_Accounts
		  ,Email_Count
		  ,LETTER_Count
		  ,CallsOffered
		  ,CallsAbandoned
		  ,CallsAnswered
		  ,CallsAnsweredIn20s
		  ,IBRPCs
		  ,IBPaymentPTP
		  ,PaymentPTP
		  ,PaymentPTPAmt
		  ,OBCallsAbandoned
		  ,OBAttemptedOperatorTransfers
		  ,OBCallsAnswered
		  ,OBRPCs
		  ,OBPaymentPTP
		  ,OBPaymentAmt
		  ,SystemHrs
		  ,ReadyHrs
		  ,WrapupHrs
		  ,AccountsWorked
		  ,SMSSent
	 FROM dbo.#PenSerKpi_3
	ORDER BY 
		ServiceName,
		DateValue;

	DECLARE @tab char(1) = CHAR(9)

	DECLARE @totcntmm VARCHAR (MAX);
	SELECT @totcntmm=CAST(COUNT(DateValue) AS varchar) FROM dbo.#PenSerKpi_2
	PRINT @totcntmm

	DECLARE @body1 VARCHAR (MAX); 
			SET @body1 = 'Hi All,
		
	Pendricks KPI SQL code executed for '+convert(varchar,@End_Month,102)+'

	Total count of rows added are '+ (@totcntmm) +'. 

	Regards,
	Business Analytics';
			print @body1
	DECLARE @subject1 VARCHAR (MAX); 
			SET @subject1 = 'Pendricks KPI SQL code executed for ' + convert(varchar,@End_Month,102);
			print @subject1
		--send email
		if (SELECT count(DateValue) FROM dbo.#PenSerKpi_2)>0
			EXEC msdb.dbo.sp_send_dbmail
			@profile_name = 'DW Mail',--@@SERVERNAME, --'DFW2-BISQL-001',
			@from_address ='Reports SpeechAnalytics <reports.speechanalytics@radiusgs.com>',
			@recipients = 'Dyuti.Mukherji@radiusgs.com;
			Mukesh.Salunke@radiusgs.com;tushar.kumar@radiusgs.com;dw@radiusgs.com',
			@copy_recipients='Pulkit.Jain@radiusgs.com;',
			@subject = @subject1,
			@body = @body1; 

END
