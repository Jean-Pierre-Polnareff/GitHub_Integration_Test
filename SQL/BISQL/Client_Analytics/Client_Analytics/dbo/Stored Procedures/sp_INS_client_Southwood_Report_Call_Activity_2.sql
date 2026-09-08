






CREATE PROCEDURE [dbo].[sp_INS_client_Southwood_Report_Call_Activity]
@StartDate			datetime = NULL, 
@EndDate			datetime = NULL 

AS 


BEGIN
SET NOCOUNT ON; 

	DECLARE @Exe_Date DATETIME
	SET @Exe_Date=GETDATE()-3
	PRINT @Exe_Date

	DECLARE @Start_Date DATETIME
	SET @Start_Date=CAST(DATEADD(DAY,1,EOMONTH(@Exe_Date,-1)) as date)
	PRINT @Start_Date

	DECLARE @End_Date DATETIME
	SET @End_Date=CAST(EOMONTH(@Exe_Date,0) as date)
	PRINT @End_Date

DECLARE 
@avFeedName VARCHAR (200) =  'Southwood_Call_Activity_' + FORMAT(getdate(), 'MMddyy'),
@dtFeedDate DATETIME = GETDATE(),
@vTemplate	VARCHAR(150) = '\\DFW2-BISQL-001\SSISFlatFileStage_Offshore\Exports\Southwood\Template\Southwood_Call_Activity.xls',
@vSQL		VARCHAR(8000),
@vSubject	VARCHAR(500) = @@SERVERNAME + '.' + DB_NAME() + '.dbo.' + OBJECT_NAME(@@PROCID), 
@vFile		VARCHAR(255)
 
declare @vCall_Monthly	VARCHAR(100) = Replace(@avFeedName,'.csv','.xls') ;
select @vFile = '\\DFW2-BISQL-001\SSISFlatFileStage_Offshore\Exports\Southwood\Work\' + @vCall_Monthly + '.xls'

	--Extracting Artiva CRM Data to get original account number
	IF OBJECT_ID('tempdb.dbo.#Results1') IS NOT NULL 
	DROP TABLE dbo.#Results1
	SELECT ARACID AS CustomerID, ZZACORIGCREDACNUM as [Original Creditor Account Number] , ZZACORIGCREDNM as OriginalCreditor, ARACCLACCT as ORNGLACC
	INTO dbo.#Results1
	FROM OPENQUERY([THIRDPROD], 'SELECT ARACID, ZZACORIGCREDACNUM , ZZACORIGCREDNM , ARACCLACCT
	FROM SQLUser.ARACCOUNT WHERE ARACCLTID  IN (''STHWOD'', ''STHWD2'', ''STHWD3'', ''STHWD4'', ''STHWOS'')')

	--Extracting Artiva CRM Data to get customer state
	IF OBJECT_ID('tempdb.dbo.#Results2') IS NOT NULL 
	DROP TABLE dbo.#Results2
	SELECT cast(CustomerId as varchar(max))CustomerId
		  ,CustomerState
	INTO dbo.#Results2
	FROM [DW_MSTR_DM].[dbo].DimCustomer
	WHERE ClientId IN ('STHWOD', 'STHWD2', 'STHWD3', 'STHWD4', 'STHWOS')

	IF OBJECT_ID('tempdb.dbo.#AccRela1') IS NOT NULL 
	DROP TABLE dbo.#AccRela1
	SELECT DISTINCT ARACID as CustomerID
				   ,ARACRPID as RelapID
				   ,ARENPH as Phone_Dialed
				   ,ARRELTYPID as RelapType 
	into dbo.#AccRela1
	FROM OPENQUERY([THIRDPROD],' 
	SELECT
	account.ARACID
	,ARREL.ARRELTYPID
	,account.ARACRPID
	,arentity.ARENPH 
	FROM ARCLIENT clientinfo INNER JOIN SQLUser.ARACCOUNT account ON clientinfo.ARCLID = account.ARACCLTID
	Inner JOIN SQLUser.ARRELATIONSHIP ARREL ON account.ARACID = ARREL.ARRELACID
	INNER JOIN SQLUser.ARENTITY arentity ON arentity.ARENID = ARREL.ARRELENID
	LEFT JOIN ZZENSKIPPHONES skipphone ON arentity.ARENID = skipphone.ZZENSKIPPHENID 
	WHERE account.ARACCLTID IN (''STHWOD'', ''STHWD2'', ''STHWD3'', ''STHWD4'', ''STHWOS'')')
	WHERE ARENPH IS NOT NULL

	IF OBJECT_ID('tempdb.dbo.#AccRela2') IS NOT NULL 
	DROP TABLE dbo.#AccRela2
	SELECT DISTINCT ARACID as CustomerID
				   ,ARACRPID as RelapID
				   ,ARENPH2 as Phone_Dialed
				   ,ARRELTYPID as RelapType 
	into dbo.#AccRela2
	FROM OPENQUERY([THIRDPROD],' 
	SELECT
	account.ARACID,ARRELTYPID
	,account.ARACRPID 
	,arentity.ARENPH2 
	FROM ARCLIENT clientinfo INNER JOIN SQLUser.ARACCOUNT account ON clientinfo.ARCLID = account.ARACCLTID
	Inner JOIN SQLUser.ARRELATIONSHIP ARREL ON account.ARACID = ARREL.ARRELACID
	INNER JOIN SQLUser.ARENTITY arentity ON arentity.ARENID = ARREL.ARRELENID
	LEFT JOIN ZZENSKIPPHONES skipphone ON arentity.ARENID = skipphone.ZZENSKIPPHENID 
	WHERE account.ARACCLTID IN (''STHWOD'', ''STHWD2'', ''STHWD3'', ''STHWD4'', ''STHWOS'')')
	WHERE ARENPH2 IS NOT NULL

	IF OBJECT_ID('tempdb.dbo.#AccRela3') IS NOT NULL 
	DROP TABLE dbo.#AccRela3
	SELECT DISTINCT ARACID as CustomerID
				   ,ARACRPID as RelapID
				   ,ARENPH3 as Phone_Dialed
				   ,ARRELTYPID as RelapType 
	into dbo.#AccRela3
	FROM OPENQUERY([THIRDPROD],' 
	SELECT
	account.ARACID
	,ARREL.ARRELTYPID
	,account.ARACRPID 
	,arentity.ARENPH3  
	FROM ARCLIENT clientinfo INNER JOIN SQLUser.ARACCOUNT account ON clientinfo.ARCLID = account.ARACCLTID
	Inner JOIN SQLUser.ARRELATIONSHIP ARREL ON account.ARACID = ARREL.ARRELACID
	INNER JOIN SQLUser.ARENTITY arentity ON arentity.ARENID = ARREL.ARRELENID
	LEFT JOIN ZZENSKIPPHONES skipphone ON arentity.ARENID = skipphone.ZZENSKIPPHENID 
	WHERE account.ARACCLTID IN (''STHWOD'', ''STHWD2'', ''STHWD3'', ''STHWD4'', ''STHWOS'')')
	WHERE ARENPH3 IS NOT NULL


	IF OBJECT_ID('tempdb.dbo.#AccRela4') IS NOT NULL 
	DROP TABLE dbo.#AccRela4
	SELECT DISTINCT ARACID as CustomerID
				   ,ARACRPID as RelapID
				   ,ARENPOEPH as Phone_Dialed
				   ,ARRELTYPID as RelapType 
	into dbo.#AccRela4
	FROM OPENQUERY([THIRDPROD],' 
	SELECT
	account.ARACID
	,ARREL.ARRELTYPID
	,account.ARACRPID 
	,arentity.ARENPOEPH  
	FROM ARCLIENT clientinfo INNER JOIN SQLUser.ARACCOUNT account ON clientinfo.ARCLID = account.ARACCLTID
	Inner JOIN SQLUser.ARRELATIONSHIP ARREL ON account.ARACID = ARREL.ARRELACID
	INNER JOIN SQLUser.ARENTITY arentity ON arentity.ARENID = ARREL.ARRELENID
	LEFT JOIN ZZENSKIPPHONES skipphone ON arentity.ARENID = skipphone.ZZENSKIPPHENID 
	WHERE account.ARACCLTID IN (''STHWOD'', ''STHWD2'', ''STHWD3'', ''STHWD4'', ''STHWOS'')')
	WHERE ARENPOEPH IS NOT NULL

	IF OBJECT_ID('tempdb.dbo.#AccRela5') IS NOT NULL 
	DROP TABLE dbo.#AccRela5
	SELECT DISTINCT ARACID as CustomerID
				   ,ARACRPID as RelapID
				   ,ZZENDSAPH as Phone_Dialed
				   ,ARRELTYPID as RelapType 
	into dbo.#AccRela5
	FROM OPENQUERY([THIRDPROD],'
	SELECT
	account.ARACID
	,ARREL.ARRELTYPID
	,account.ARACRPID
	,arentity.ZZENDSAPH
	FROM ARCLIENT clientinfo INNER JOIN SQLUser.ARACCOUNT account ON clientinfo.ARCLID = account.ARACCLTID
	Inner JOIN SQLUser.ARRELATIONSHIP ARREL ON account.ARACID = ARREL.ARRELACID
	INNER JOIN SQLUser.ARENTITY arentity ON arentity.ARENID = ARREL.ARRELENID
	LEFT JOIN ZZENSKIPPHONES skipphone ON arentity.ARENID = skipphone.ZZENSKIPPHENID 
	WHERE account.ARACCLTID IN (''STHWOD'', ''STHWD2'', ''STHWD3'', ''STHWD4'', ''STHWOS'')')
	WHERE ZZENDSAPH IS NOT NULL



	IF OBJECT_ID('tempdb.dbo.#AccRela6') IS NOT NULL 
	DROP TABLE dbo.#AccRela6
	SELECT DISTINCT ARACID as CustomerID
				   ,ARACRPID as RelapID
				   ,ZZENPOEPH2 as Phone_Dialed
				   ,ARRELTYPID as RelapType 
	into dbo.#AccRela6
	FROM OPENQUERY([THIRDPROD],'
	SELECT
	account.ARACID
	,ARREL.ARRELTYPID
	,account.ARACRPID
	,arentity.ZZENPOEPH2
	FROM ARCLIENT clientinfo INNER JOIN SQLUser.ARACCOUNT account ON clientinfo.ARCLID = account.ARACCLTID
	Inner JOIN SQLUser.ARRELATIONSHIP ARREL ON account.ARACID = ARREL.ARRELACID
	INNER JOIN SQLUser.ARENTITY arentity ON arentity.ARENID = ARREL.ARRELENID
	LEFT JOIN ZZENSKIPPHONES skipphone ON arentity.ARENID = skipphone.ZZENSKIPPHENID 
	WHERE account.ARACCLTID IN (''STHWOD'', ''STHWD2'', ''STHWD3'', ''STHWD4'', ''STHWOS'')')
	WHERE ZZENPOEPH2 IS NOT NULL



	IF OBJECT_ID('tempdb.dbo.#AccRela7') IS NOT NULL 
	DROP TABLE dbo.#AccRela7
	SELECT DISTINCT ARACID as CustomerID
				   ,ARACRPID as RelapID
				   ,ZZENPOEPH3 as Phone_Dialed
				   ,ARRELTYPID as RelapType 
	into dbo.#AccRela7
	FROM OPENQUERY([THIRDPROD],'
	SELECT account.ARACID
		,ARREL.ARRELTYPID
		,account.ARACRPID
		,arentity.ZZENPOEPH3 
	FROM ARCLIENT clientinfo 
		INNER JOIN SQLUser.ARACCOUNT account ON clientinfo.ARCLID = account.ARACCLTID 
		INNER JOIN SQLUser.ARRELATIONSHIP ARREL ON account.ARACID = ARREL.ARRELACID 
		INNER JOIN SQLUser.ARENTITY arentity ON arentity.ARENID = ARREL.ARRELENID 
		LEFT JOIN ZZENSKIPPHONES skipphone ON arentity.ARENID = skipphone.ZZENSKIPPHENID 
	WHERE account.ARACCLTID IN (''STHWOD'', ''STHWD2'', ''STHWD3'', ''STHWD4'', ''STHWOS'')')
	WHERE ZZENPOEPH3 IS NOT NULL

	IF OBJECT_ID('tempdb.dbo.#AccRela8') IS NOT NULL 
	DROP TABLE dbo.#AccRela8
	SELECT DISTINCT ARACID as CustomerID
				   ,ARACRPID as RelapID
				   ,ZZENSKIPPHPHNNUM as Phone_Dialed
				   ,ARRELTYPID as RelapType 
	into dbo.#AccRela8
	FROM OPENQUERY([THIRDPROD],'
	SELECT account.ARACID
		  ,ARREL.ARRELTYPID
		  ,account.ARACRPID
		  ,skipphone.ZZENSKIPPHPHNNUM 
	FROM ARCLIENT clientinfo 
		INNER JOIN SQLUser.ARACCOUNT account ON clientinfo.ARCLID = account.ARACCLTID
		INNER JOIN SQLUser.ARRELATIONSHIP ARREL ON account.ARACID = ARREL.ARRELACID
		INNER JOIN SQLUser.ARENTITY arentity ON arentity.ARENID = ARREL.ARRELENID
		LEFT JOIN ZZENSKIPPHONES skipphone ON arentity.ARENID = skipphone.ZZENSKIPPHENID 
	WHERE account.ARACCLTID IN (''STHWOD'', ''STHWD2'', ''STHWD3'', ''STHWD4'', ''STHWOS'')')
	WHERE ZZENSKIPPHPHNNUM IS NOT NULL

	IF OBJECT_ID('tempdb.dbo.#AccRela9') IS NOT NULL 
	DROP TABLE dbo.#AccRela9
	SELECT DISTINCT ARACID as CustomerID
				   ,ARACRPID as RelapID
				   ,ZZENTXTMSGNUM as Phone_Dialed
				   ,ARRELTYPID as RelapType 
	into dbo.#AccRela9
	FROM OPENQUERY([THIRDPROD],'
	SELECT account.ARACID
		   ,ARREL.ARRELTYPID
		   ,account.ARACRPID
		   ,arentity.ZZENTXTMSGNUM 
	FROM ARCLIENT clientinfo 
		 INNER JOIN SQLUser.ARACCOUNT account ON clientinfo.ARCLID = account.ARACCLTID
		 INNER JOIN SQLUser.ARRELATIONSHIP ARREL ON account.ARACID = ARREL.ARRELACID
		 INNER JOIN SQLUser.ARENTITY arentity ON arentity.ARENID = ARREL.ARRELENID
		 LEFT JOIN ZZENSKIPPHONES skipphone ON arentity.ARENID = skipphone.ZZENSKIPPHENID 
	WHERE account.ARACCLTID IN (''STHWOD'', ''STHWD2'', ''STHWD3'', ''STHWD4'', ''STHWOS'')')
	WHERE ZZENTXTMSGNUM IS NOT NULL

	IF OBJECT_ID('tempdb.dbo.#AccRela') IS NOT NULL 
	DROP TABLE dbo.#AccRela
	SELECT DISTINCT CustomerID
				   ,Phone_Dialed
				   ,RelapType 
	INTO dbo.#AccRela
	FROM
	(SELECT CustomerID,RelapID,Phone_Dialed,RelapType FROM dbo.#AccRela1
	UNION 
	SELECT CustomerID,RelapID,Phone_Dialed,RelapType FROM dbo.#AccRela2
	UNION
	SELECT CustomerID,RelapID,Phone_Dialed,RelapType FROM dbo.#AccRela3
	UNION
	SELECT CustomerID,RelapID,Phone_Dialed,RelapType FROM dbo.#AccRela4
	UNION
	SELECT CustomerID,RelapID,Phone_Dialed,RelapType FROM dbo.#AccRela5
	UNION
	SELECT CustomerID,RelapID,Phone_Dialed,RelapType FROM dbo.#AccRela6
	UNION
	SELECT CustomerID,RelapID,Phone_Dialed,RelapType FROM dbo.#AccRela7
	UNION
	SELECT CustomerID,RelapID,Phone_Dialed,RelapType FROM dbo.#AccRela8
	UNION
	SELECT CustomerID,RelapID,Phone_Dialed,RelapType FROM dbo.#AccRela9
	) a


	--combining for data upload
	IF OBJECT_ID('tempdb.dbo.#Results3') IS NOT NULL 
	DROP TABLE dbo.#Results3
	SELECT rc.Account_Number
		  ,ocb.ORNGLACC
		  ,ocb.OriginalCreditor
		  ,ocb.[Original Creditor Account Number]
		  ,rc.Transaction_Type
		  ,rc.Livevox_Result
		  ,CASE WHEN rc.[Is_RPC]=1 THEN 'Yes' ELSE 'No' END as [Is_RPC]
		  ,CASE WHEN rc.[Is_Promise]=1 THEN 'Yes' ELSE 'No' END as [Is_Promise]
		  ,CAST(rc.[Call_Date] as date) as [Call_Date]
		  ,CAST(rc.[Call_Connect_Time_CT] as time) [Call_Connect_Time_CT]
		  ,CAST(rc.[Call_End_Time] as time) [Call_End_Time]
		  ,'CST' AS 'Time_Zone'
		  ,rc.[Phone_Dialed]
		  ,rc.[Call_Duration]
		  ,rc.[Session_Id]
		  ,rc.[Agent_Full_Name]
		  ,dcu.[CustomerState]
		  ,CASE WHEN rt.RelapType IS NULL THEN 'PRIM' ELSE rt.RelapType END RelapType
		  ,row_number() over (partition by rc.[Session_Id] order by rt.RelapType desc) rnk
		INTO dbo.#Results3
	  FROM [DW_MSTR_DM].[dbo].[RadiusCall] rc LEFT JOIN 
			dbo.#Results1 ocb on rc.Account_Number=cast(ocb.CustomerID as varchar(max))
			left join dbo.#Results2 dcu on rc.Account_Number=dcu.CustomerId
			left join dbo.#AccRela rt on rc.Account_Number=cast(rt.CustomerID as varchar(max))
				and rc.Phone_Dialed=rt.Phone_Dialed
	  WHERE rc.call_date BETWEEN @Start_Date and @End_Date
		and rc.[Service_Id] IN (152014,152272,152015,152016,152017,152018,152271)
		and ocb.ORNGLACC is not null
		ORDER BY rc.call_date



	DELETE FROM CLIENT_ANALYTICS.dbo.RPT_client_Southwood_Call_Activity
	WHERE Call_Date BETWEEN @Start_Date and @End_Date

	INSERT INTO CLIENT_ANALYTICS.dbo.RPT_client_Southwood_Call_Activity
	(
	Account_Number
		  ,ORNGLACC
		  ,[Original Creditor]
		  ,[Original Creditor Account Number]
		  ,Transaction_Type
		  ,Livevox_Result
		  ,Is_RPC
		  ,Is_Promise
		  ,Call_Date
		  ,Call_Connect_Time_CT
		  ,Call_End_Time
		  ,Time_Zone
		  ,Phone_Dialed
		  ,Call_Duration
		  ,Session_Id
		  ,Agent_Full_Name
		  ,CustomerState
		  ,RelapType
		  )
	SELECT Account_Number
		  ,ORNGLACC
		  ,OriginalCreditor
		  , '`' + CAST([Original Creditor Account Number] as varchar(255))  AS [Original Creditor Account Number]
		  ,Transaction_Type
		  ,Livevox_Result
		  ,Is_RPC
		  ,Is_Promise
		  ,Call_Date
		  ,Call_Connect_Time_CT
		  ,Call_End_Time
		  ,Time_Zone
		  ,Phone_Dialed
		  ,Call_Duration
		  ,Session_Id
		  ,Agent_Full_Name
		  ,CustomerState
		  ,RelapType
	 FROM dbo.#Results3
	 WHERE RNK=1

	--No of rows in the table
	DECLARE @totcnt VARCHAR (MAX);
	SELECT @totcnt=CAST(COUNT(Livevox_Result) AS varchar) FROM dbo.#Results3 WHERE RNK=1
	PRINT @totcnt

	DECLARE @tab char(1) = CHAR(9)
	DECLARE @subject VARCHAR (MAX);
	DECLARE @body VARCHAR (MAX);

	SET @subject = 'Southwood Call Activity Report for : ' + format(@Start_Date,'MMM-yyyy');

			  SET @vSQL = 'copy /Y ' + @vTemplate + ' \\DFW2-BISQL-001\SSISFlatFileStage_Offshore\Exports\Southwood\Work\' + @vCall_Monthly + '.xls';
      print @vSQL
	  EXEC xp_cmdshell @vSQL, no_output;
	      SET @vSQL = 'INSERT INTO OPENROWSET(''Microsoft.ACE.OLEDB.12.0'',''Excel 12.0;Database=\\DFW2-BISQL-001\SSISFlatFileStage_Offshore\Exports\Southwood\Work\' 
	  + @vCall_Monthly + '.xls;'',''SELECT * FROM [Southwood_Call_Activity$]'') SELECT Account_Number AS ''Service Providers Account Number''
	      ,[Original Creditor] ''Original Creditor''
		  ,'+'''`'''+'+'+'CAST(ORNGLACC as varchar(255)) as ''Southwood Account Number''
		  ,[Original Creditor Account Number] as ''Original Creditor Account Number''
		  ,Transaction_Type as ''Call Direction''
		  ,Livevox_Result as ''Result Code''
		  ,[Is_RPC] ''Right Party Contact''
		  ,[Is_Promise] ''Promise to Pay''
		  ,[Call_Date]''Date of Call''
		  ,FORMAT(CAST([Call_Connect_Time_CT] as datetime),''hh:mm:ss tt'') ''Start Time of Call''
		  ,FORMAT(CAST([Call_End_Time] as datetime),''hh:mm:ss tt'') ''End Time of Call''
		  ,Time_Zone AS ''Time Zone''
		  ,[Phone_Dialed] as ''Phone Number Dialed''
		  ,[Call_Duration] as ''Call Duration''
		  ,[Session_Id] as ''RGS Call Identifier''
		  ,[RelapType] as ''Name Associated''
		  ,[Agent_Full_Name] as ''Collector''''s Name''
		  ,[CustomerState] as ''State''
	  FROM dbo.RPT_client_Southwood_Call_Activity
	  WHERE call_date BETWEEN '+''''+convert(varchar,@Start_Date,23)+''' and '+''''+convert(varchar,@End_Date,23)+''' 
		ORDER BY call_date';
	  PRINT @vSQL
	  EXEC (@vSQL)
	SET @body = 'Hi All,
		
	Please find attached spreadsheet with Call Activity happend for Southwood in the month of '+format(@Start_Date,'MMM-yyyy')+'.

	Total count of calls in the spreadsheet are '+ (@totcnt) +'.


	Regards,
	Business Analytics';
		
		--send email
		if (SELECT count(Livevox_Result) FROM dbo.#Results3)>0
			EXEC msdb.dbo.sp_send_dbmail
			@profile_name = 'DW Mail',--@@SERVERNAME, --'DFW2-BISQL-001',
			@from_address ='_Group - Data Warehousing <dw@radiusgs.com>',
			--@recipients = 'amod.ramugade@radiusgs.com;vladislav.pilipets@radiusgs.com,Pulkit.Jain@radiusgs.com',
			@recipients = 'clientservices1@radiusgs.com;Stanley.Martin@radiusgs.com;Patsy.Delvecchio@radiusgs.com
			;Angie.Huie@radiusgs.com;jeremiah.reichert@radiusgs.com;ClientServices-All@radiusgs.com',
			-- THIS DISTRIBUTION LIST IS FROM [Radius Daily Master Data - EXTENSION - Reporting Inventory]
			--@recipients = 'Jamie.Stamp@radiusgs.com;Stanley.Martin@radiusgs.com;Andrea.Ewing@radiusgs.com;
			--		   Christi.Regan@radiusgs.com;jeremiah.reichert@radiusgs.com;ClientServices-All@radiusgs.com',
			--@copy_recipients='ted.miller@radiusgs.com;Pulkit.Jain@radiusgs.com;dw@radiusgs.com',
			@copy_recipients='Pulkit.Jain@radiusgs.com;dw@radiusgs.com',
			@subject = @subject,
			@body = @body,
			--@query = @query , 
			@execute_query_database='CLIENT_ANALYTICS'
			,@query_result_header=1
		   ,@file_attachments = @vFile
		   ,@query_result_separator=@tab
		   ,@query_result_no_padding=1 
		   ,@query_result_width=32767;

END