
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
-- =============================================
-- Author:		<Author,,Name>
-- Create date: <Create Date,,>
-- Description:	<Description,,>
-- =============================================
CREATE PROCEDURE usp_62_Rpt_SMS_Summary_Daily 
	
AS
BEGIN
	SET NOCOUNT ON;

    --Setting a maxdate for automatic run
	DECLARE @maxdate DATETIME
	SELECT @maxdate=CAST(max([DateStarted]) as date) 
			from [DW_MSTR_DM].[dbo].[FactLIveVoxSMSSummary]
	PRINT @maxdate

	--Extracting required Service IDs
	IF OBJECT_ID('tempdb.dbo.#DailySmsDt') IS NOT NULL 
	DROP TABLE dbo.#DailySmsDt
	SELECT RepExeDate
		  ,[ServiceID]
		  ,[CallCenter]
		  ,[CampaignName]
		  ,[CampaignType]
		  ,[Site]
		  ,Total_DIALED
		  ,Total_Connected
		  ,Total_Stop
		  ,[TotalCalls]
		  ,[RPCs]
		  ,[Payments]
		  ,[PaymentAmt]
	INTO dbo.#DailySmsDt
	FROM
	(SELECT a.[Call_Date] AS RepExeDate
		  ,b.[ServiceID]
		  ,b.[CallCenter]
		  ,b.[CampaignName]
		  ,b.[CampaignType]
		  ,b.[Site]
		,SUM(CASE WHEN a.Livevox_Result in('Consumer responded Help to text before Authentication'
			,'Consumer responded Stop to text','SMS MO Received','SMS MT Delivered'
			,'SMS MT Failed','Operator Transfer') THEN 1 ELSE 0 END) as Total_DIALED
		,SUM(CASE WHEN Livevox_Result in('SMS MT Delivered','Operator Transfer') 
			THEN 1 ELSE 0 END) as Total_Connected
		,SUM(CASE WHEN Livevox_Result in('Consumer responded Stop to text',
			'Consumer responded Help to text before Authentication') 
			THEN 1 ELSE 0 END) as Total_Stop
		,0 as [TotalCalls]
		,0 as [RPCs]
		,0 as  [Payments]
		,0 as [PaymentAmt]
	  FROM [DW_MSTR_DM].[dbo].[RadiusCall] a with (nolock)
	  LEFT JOIN [CLIENT_ANALYTICS].[dbo].[DimServiceNames] b on
	  A.Service_Id=b.ServiceID
	  WHERE a.[Call_Date]=@maxdate
	  AND b.[ServiceType] IN ('OB-HTI')
	  group by a.[Call_Date]
		  ,b.[ServiceID]
		  ,b.[CallCenter]
		  ,b.[CampaignName]
		  ,b.[CampaignType]
		  ,b.[Site]
	UNION
	SELECT CAST(a.[DateStarted] as date) AS RepExeDate
		  ,b.[ServiceID]
		  ,b.[CallCenter]
		  ,b.[CampaignName]
		  ,b.[CampaignType]
		  ,b.[Site]
		  ,0 as Total_DIALED
		  ,0 as Total_Connected
		  ,0 as Total_Stop
		  ,SUM(a.[TotalCalls]) [TotalCalls]
		  ,SUM(a.[RPCs]) [RPCs]
		  ,SUM(a.[Payments]) [Payments]
		  ,SUM(a.[PaymentAmt]) [PaymentAmt]
	  FROM DW_MSTR_DM.[dbo].[FactLIveVoxSMSSummary] a
	   LEFT JOIN [CLIENT_ANALYTICS].[dbo].[DimServiceNames] b on
	  A.[ServiceID]=b.ServiceID
	  WHERE CAST(a.[DateStarted]as date)=@maxdate
	  AND b.[ServiceType] IN ('IB-SMS','IB-EMAIL')
	  group by a.[DateStarted]
		  ,b.[ServiceID]
		  ,b.[CallCenter]
		  ,b.[CampaignName]
		  ,b.[CampaignType]
		  ,b.[Site]) c

	DELETE FROM CLIENT_ANALYTICS.dbo.Rpt_Daily_SMS_Summary
	WHERE RepExeDate>=@maxdate

	INSERT INTO CLIENT_ANALYTICS.dbo.Rpt_Daily_SMS_Summary
	SELECT RepExeDate
		  ,[ServiceID]
		  ,[CallCenter]
		  ,[CampaignName]
		  ,[CampaignType]
		  ,[Site]
		  ,Total_DIALED
		  ,Total_Connected
		  ,Total_Stop
		  ,[TotalCalls]
		  ,[RPCs]
		  ,[Payments]
		  ,[PaymentAmt]
	FROM dbo.#DailySmsDt

	DECLARE @tab char(1) = CHAR(9)

	DECLARE @totcntmm VARCHAR (MAX);
	SELECT @totcntmm=CAST(COUNT([ServiceID]) AS varchar) FROM dbo.#DailySmsDt
	PRINT @totcntmm

	DECLARE @body1 VARCHAR (MAX); 
			SET @body1 = 'Hi All,
		
	Daily SMS Summary SQL code executed for '+convert(varchar,@maxdate,102)+'

	Total count of rows added are '+ (@totcntmm) +'. 

	Regards,
	Business Analytics';
			print @body1
	DECLARE @subject1 VARCHAR (MAX); 
			SET @subject1 = 'Daily SMS Summary SQL code executed for ' + convert(varchar,@maxdate,102);
			print @subject1
		--send email
		if (SELECT count(ServiceID) FROM dbo.#DailySmsDt)>0
			EXEC msdb.dbo.sp_send_dbmail
			@profile_name = 'DW Mail',--@@SERVERNAME, --'DFW2-BISQL-001',
			@from_address ='Reports SpeechAnalytics <reports.speechanalytics@radiusgs.com>',
			@recipients = 'business.analytics@radiusgs.com;dw@radiusgs.com',
			@copy_recipients='Pulkit.Jain@radiusgs.com;',
			@subject = @subject1,
			@body = @body1;
END
GO
