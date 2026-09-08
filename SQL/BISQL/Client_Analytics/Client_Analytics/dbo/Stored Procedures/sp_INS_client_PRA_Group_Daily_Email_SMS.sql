




-- ====================================================================
--  Populate daily Email and SMS summary for PRA Group client 
-- ====================================================================

CREATE PROCEDURE [dbo].[sp_INS_client_PRA_Group_Daily_Email_SMS]


--@StartDate			date = NULL


AS 


BEGIN

	SET NOCOUNT ON; 

	DECLARE @StartDate date =  CAST(DATEADD(DAY,-1,GETDATE()) AS date)
	

	DELETE FROM [CLIENT_ANALYTICS].[dbo].[RPT_client_PRA_Group_Daily_Email_SMS]
	WHERE [Date] = CAST(@StartDate as date)   


	DECLARE @EmailDataExists INT = 1;
	DECLARE @SMSDataExists INT = 1;
	DECLARE @subject_Email VARCHAR(50) = 'PRA Group Email data missing for ' + convert(VARCHAR,@startdate,1);
	DECLARE @subject_SMS VARCHAR(50) = 'PRA Group SMS data missing for ' + convert(VARCHAR,@startdate,1);
	DECLARE @body_Email VARCHAR(50) = 'PRA Group Email data for ' + convert(VARCHAR,@startdate,1) + ' is missing.';
	DECLARE @body_SMS VARCHAR(50) = 'PRA Group SMS data for ' + convert(VARCHAR,@startdate,1) + ' is missing.';



		

	SET NOCOUNT ON;


	----Daily sent SMS data with Livevox result 'SMS MT Delivered' or 'SMS MT Failed'

IF OBJECT_ID('tempdb..#act_prev') IS NOT NULL
	DROP TABLE #act_prev;
	SELECT cast(dcu.CustomerId as varchar(max))CustomerId
		--, dcu.ClientAccountNumber
		--, Concat(dcu.FirstName ,' ', dcu.LastName) AS Consumer_Name  
		,dcu.keysourcesystem
		, dcu.KeyCustomer
		, dcl.ClientParent
		, dcl.ClientId
		, dcl.ClientStreamId
		, dcl.ClientStream
		--, dcu.StatusCode
		--, dcu.SourceSystem
		--, dcu.ListDate
		--, dcu.InitialBalance
	INTO #act_prev
	FROM [DW_MSTR_DM].[dbo].DimCustomer dcu WITH (NOLOCK)
			JOIN DW_MSTR_DM.dbo.DimClient dcl WITH (NOLOCK) 
			ON dcu.ClientId = dcl.ClientId
		   AND dcu.sourcesystem = dcl.sourcesystem
	WHERE dcl.ClientStreamId = 'IMPRAGROUP'
		  AND dcu.keysourcesystem = 2
		--  select * from #act_prev
 
	  		IF OBJECT_ID('tempdb..#rcalls_raw') IS NOT NULL
		DROP TABLE #rcalls_raw;	      
		SELECT rc.Call_Date  AS [Date of SMS]	 
			 , a.ClientId 
			 , rc.Account_Number AS [Account Number] 
			 , ISNULL(SUM(CASE WHEN rc.livevox_result = 'SMS MT Delivered' THEN 1 ELSE 0 END), 0) AS SMS_Delivered
			 , ISNULL(SUM(CASE WHEN rc.livevox_result = 'SMS MT Failed' THEN 1 ELSE 0 END), 0) AS SMS_Failed
		into #rcalls_raw
		FROM [DW_MSTR_DM].[dbo].[RadiusCall] rc WITH (NOLOCK)  
		LEFT JOIN #act_prev a 
		on rc.Account_Number=cast(a.CustomerID as varchar(max))
		WHERE 
		  rc.Call_Date =  @startdate
		AND rc.[livevox_result] IN ('SMS MT Delivered', 'SMS MT Failed')
		AND rc.Creditor_Code=a.ClientId
		AND a.CustomerId IS NOT NULL
		GROUP BY
			   rc.Call_Date   
			 , a.ClientId 
			 , rc.Account_Number
 
 
		--SELECT * FROM #rcalls_raw order by [Date of SMS]
 
 
 
	--------------------------------------------------Get yesterday's Email data for PRA Group-------------------------------------------------------------
	DROP TABLE IF EXISTS #emd
	SELECT Cast(emd.Send_date AS Date) AS [Date]
	, dcl.clientid AS ClientID
	, emd.customerid AS [Account Number]
	, ISNULL(SUM(emd.Delivered), 0) AS [Emails Delivered]
	, ISNULL(SUM(emd.unq_opens), 0) AS [Emails Unique Opened] 
	, ISNULL(SUM(emd.total_opens), 0) AS [Email Total Opens] 
	, ISNULL(SUM(emd.clicks), 0) AS [Email Total Clicks] 
	INTO #emd
	FROM CLIENT_ANALYTICS.dbo.RPT_email_guid (NOLOCK) emd
		JOIN DW_MSTR_DM.dbo.DimClient dcl
	ON emd.clientid = dcl.ClientId
	WHERE emd.Send_date  = @startdate     
	AND dcl.ClientStreamId = 'IMPRAGROUP'
	AND emd.Delivered > 0
	GROUP BY 
				Cast(emd.Send_date AS Date) 
			, dcl.clientid 
			, emd.customerid
 
	--SELECT * FROM #emd order by[date]
 
 
	---------------------------------------------Combine yesterday's Email-SMS data for PRA Group-----------------------------------------------------------------------
	DROP TABLE IF EXISTS #result
	SELECT  COALESCE(e.[Date], t.[Date of SMS], @startdate) AS [Date]
	, COALESCE(e.ClientID, t.ClientId, dcl.ClientId) AS ClientID
	, COALESCE(e.[Account Number], t.[Account Number]) AS [Account Number]
	, ISNULL(e.[Emails Delivered], 0) AS [Emails Delivered]
	, ISNULL(e.[Emails Unique Opened],0) AS [Emails Unique Opened]
	, ISNULL(e.[Email Total Opens], 0) AS [Email Total Opens] 
	, ISNULL(e.[Email Total Clicks], 0) AS [Email Total Clicks] 
	, ISNULL(t.SMS_Delivered,0) AS [SMS_Delivered]
	, ISNULL(t.SMS_Failed,0) AS [SMS_Failed]
	INTO #result 
	FROM #emd e 
	FULL OUTER JOIN #rcalls_raw t
	ON e.[Date] = t.[Date of SMS]
	AND e.ClientID = t.ClientId
	AND e.[Account Number] = t.[Account Number]
	FULL OUTER JOIN DW_MSTR_DM.dbo.DimClient dcl
    ON dcl.ClientID = COALESCE(e.ClientID, t.ClientId)
	AND t.[Date of SMS] = @startdate
	AND dcl.ClientStreamId = 'IMPRAGROUP'
	WHERE COALESCE(e.[Account Number], t.[Account Number]) IS NOT NULL;

	--SELECT * FROM #result order by[date]

	INSERT INTO  [CLIENT_ANALYTICS].[dbo].[RPT_client_PRA_Group_Daily_Email_SMS]
		(
		[Date]
		, [ClientID]
		, [Account Number]
		, [Emails Delivered]
		, [Emails Unique Opened]
		, [Email Total Opens] 
		, [Email Total Clicks] 
		, [SMS_Delivered]
		, [SMS_Failed]
		)
	SELECT 	  [Date] 
		, [ClientID]
		, [Account Number]
		, [Emails Delivered]
		, [Emails Unique Opened]
		, [Email Total Opens] 
		, [Email Total Clicks] 
		, [SMS_Delivered]
		, [SMS_Failed]
	FROM #result
	WHERE [Account Number] is not null



	IF  (SELECT ISNULL(SUM([Emails Delivered]),0) FROM [CLIENT_ANALYTICS].[dbo].[RPT_client_PRA_Group_Daily_Email_SMS] WHERE [Date] = @StartDate) = 0
	BEGIN
		SET @EmailDataExists = 0;
		EXEC msdb.dbo.sp_send_dbmail
			@profile_name= @@SERVERNAME,
			@recipients='dw@radiusgs.com',
			@subject= @subject_Email,
			@body= @body_Email;
	END

	IF (SELECT ISNULL(SUM([SMS_Delivered]),0) + ISNULL(SUM([SMS_Failed]),0) FROM [CLIENT_ANALYTICS].[dbo].[RPT_client_PRA_Group_Daily_Email_SMS] WHERE [Date] = @StartDate) = 0 
	BEGIN
		SET @SMSDataExists = 0;
		EXEC msdb.dbo.sp_send_dbmail
			@profile_name= @@SERVERNAME,
			@recipients='dw@radiusgs.com',
			@subject= @subject_SMS,
			@body= @body_SMS;
	END

	DECLARE @tab char(1) = CHAR(9)
	DECLARE @subject VARCHAR (MAX);
	DECLARE @body VARCHAR (MAX);
	DECLARE @query_attachment_filename VARCHAR (MAX);

	SET @subject = 'PRA Group daily Email and SMS for : ' + convert(varchar,@startdate,106);
	
	--Query to get the yesterday data
	DECLARE @query VARCHAR (MAX); 
		SELECT  @query = 'SELECT 	  [Date] AS ''Date''
			, [ClientID] AS ''ClientID''
			, [Account Number] AS ''Account Number''
			, [Emails Delivered] AS ''Emails Delivered''
			, [Emails Unique Opened] AS ''Emails Unique Opened''
			, [Email Total Opens]  AS ''Email Total Opens''
			, [Email Total Clicks] AS ''Email Total Clicks''
			, [SMS_Delivered] AS ''SMS_Delivered''
			, [SMS_Failed] AS ''SMS_Failed''
		FROM dbo.RPT_client_PRA_Group_Daily_Email_SMS
	WHERE [Date] =' + ''''+ convert(varchar,@StartDate,23) + '''';
	SET @body = 'Hi Mark,
		
	Please find attached spreadsheet with the Email and SMS data for '+convert(varchar,@startdate,106)+'.

	
	Thanks,
	Business Analytics';
	
	SET @query_attachment_filename = 'PRA_Group_Daily_Email_SMS_'+FORMAT(@StartDate, 'yyyyMMdd')+ '.csv'
		
		
		--send email
		IF (SELECT count(ClientID)   FROM [CLIENT_ANALYTICS].[dbo].[RPT_client_PRA_Group_Daily_Email_SMS] WHERE [Date] = @StartDate) > 0
		BEGIN
			EXEC msdb.dbo.sp_send_dbmail
			@profile_name = 'DW Mail',--@@SERVERNAME, --'DFW2-BISQL-001',
			@from_address ='_Group - Data Warehousing <dw@radiusgs.com>',
			--@recipients = 'Amod.Ramugade@radiusgs.com',
			@recipients = 'Mark.Kidd@radiusgs.com',
			@copy_recipients='Ted.Miller@radiusgs.com;Debbie.Stout@radiusgs.com;Pulkit.Jain@radiusgs.com;dw@radiusgs.com',
			--@copy_recipients='Amod.Ramugade@radiusgs.com',
			@subject = @subject,
			@body = @body,
			@query = @query , 
			@execute_query_database='CLIENT_ANALYTICS',
			@query_result_header=1, @attach_query_result_as_file=1
		   ,@query_attachment_filename=@query_attachment_filename
		   ,@query_result_separator=@tab
		   ,@query_result_no_padding=1 
		   ,@query_result_width=32767; 
        END



END;

GO


