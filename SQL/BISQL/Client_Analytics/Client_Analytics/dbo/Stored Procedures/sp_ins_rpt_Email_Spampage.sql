
CREATE proc [dbo].[sp_ins_rpt_Email_Spampage]
AS
BEGIN
-- getting blocked data from Jan'23 till yesterday yesterday
IF OBJECT_ID('tempdb.dbo.#ResultsBloc') IS NOT NULL 
DROP TABLE dbo.#ResultsBloc
SELECT [GUID]
      ,[LTR]
      ,[DNA]
      ,[FileNumber]
	  ,SourceSystem
	  ,KeyCustomer
	  ,KeyClient
	  ,KeySourceSystem
	  ,[DomainName]
	  ,CAST([EventDate] as date) Spam_date
  INTO dbo.#ResultsBloc
  FROM [DW_MSTR_DM].[dbo].[Radius_EmailReportData] with (nolock)
  WHERE [Vendor] Like 'Rev%'
  AND CAST ([EventDate] as date) between '2023-01-01' and GETDATE()-1
  and EventValue='Blocked'

IF OBJECT_ID('tempdb.dbo.#ResultsBloc_1') IS NOT NULL 
DROP TABLE dbo.#ResultsBloc_1
SELECT [GUID]
	  ,CAST([EventDate] as date) Delivered_date
  INTO dbo.#ResultsBloc_1
  FROM [DW_MSTR_DM].[dbo].[Radius_EmailReportData] with (nolock)
  WHERE [Vendor] Like 'Rev%'
  and EventValue='EMAIL_SENT'
  and [GUID] IN (SELECT DISTINCT [GUID] FROM dbo.#ResultsBloc)


IF OBJECT_ID('tempdb.dbo.#ResultsBloc_2') IS NOT NULL 
DROP TABLE dbo.#ResultsBloc_2
SELECT a.[GUID]
      ,a.[LTR]
      ,a.[DNA]
      ,a.[FileNumber]
	  ,CASE WHEN a.[SourceSystem] = 'MEDPROD' THEN 'Medprod Artiva'
            WHEN a.[SourceSystem] = 'THIRDPROD' THEN 'Thirdprod Artiva' 
            ELSE a.[SourceSystem] 
            END AS [SourceSystem]
	  ,a.[DomainName]
       ,a.Spam_date
	   ,DATEADD(DAY,-(DATEPART(WEEKDAY,a. Spam_date) 
		+ @@DATEFIRST - 2) % 7,a. Spam_date) AS ReportWeek
	   ,b.Delivered_date
	   ,c.ClientStream
	   ,d.InitialBalance
	   ,d.CurrentBalance
	   ,d.CustomerState
	   ,1 AS Spam_Count
	   ,Year(a.Spam_date) as Spam_Year
	   ,CASE WHEN MONTH(a.Spam_date) IN (1, 2, 3) THEN 'Q1'
        WHEN MONTH(a.Spam_date) IN (4, 5, 6) THEN 'Q2'
        WHEN MONTH(a.Spam_date) IN (7, 8, 9) THEN 'Q3'
        WHEN MONTH(a.Spam_date) IN (10, 11, 12) THEN 'Q4'
        ELSE NULL END as Spam_Quarter
		,FORMAT(EOMONTH(a.Spam_date,0),'MMM-yy') MonthEnd
		,CAST(GETDAte() as date) Report_Date
		,c.ClientId
		,c.ClientParentGroup AS ClientParent
		,c.ClientStreamId
		,d.OfficeID
INTO dbo.#ResultsBloc_2
FROM dbo.#ResultsBloc a LEFT JOIN 
	dbo.#ResultsBloc_1 b 
		on a.[GUID]=b.[GUID] LEFT JOIN 
	[DW_MSTR_DM].[dbo].[DimClient] c 
		on a.KeyClient=c.KeyClient 
		LEFT JOIN [DW_MSTR_DM].[dbo].[DimCustomer] d 
		on a.KeyClient=d.KeyClient 
			and a.KeySourceSystem=d.KeySourceSystem
			and a.keycustomer=d.keycustomer


DECLARE @mxrepwk date
SELECT @mxrepwk=MAX(ReportWeek) FROM dbo.#ResultsBloc_2
--print @mxrepwk

TRUNCATE TABLE CLient_Analytics.dbo.Rpt_Email_Spampage

INSERT INTO CLient_Analytics.dbo.Rpt_Email_Spampage
(
[GUID]
      ,[LTR]
      ,[DNA]
      ,[FileNumber]
      ,[SourceSystem]
      ,[DomainName]
      ,[Spam_date]
      ,[ReportWeek]
      ,[Delivered_date]
      ,[ClientStream]
      ,[InitialBalance]
      ,[CurrentBalance]
      ,[CustomerState]
      ,[Spam_Count]
      ,[Spam_Year]
      ,[Spam_Quarter]
      ,[MonthEnd]
      ,[Report_Date]
      ,[ClientId]
      ,[ClientParent]
      ,[asCrWeekV]
      ,[ClientStreamId]
      ,[OfficeID]
	  )
SELECT   [GUID]
		,LTR
		,DNA
		,FileNumber
		,SourceSystem
		,DomainName
		,Spam_date
		,ReportWeek
		,Delivered_date
		,ClientStream
		,InitialBalance
		,CurrentBalance
		,CustomerState
		,Spam_Count
		,Spam_Year
		,Spam_Quarter
		,MonthEnd
		,Report_Date
		,ClientId
		,ClientParent
		,CASE WHEN ReportWeek=@mxrepwk THEN 'Current' end asCrWeekV 
		,ClientStreamId
		,OfficeID
FROM dbo.#ResultsBloc_2
ORDER BY Spam_Date

DECLARE @tab char(1) = CHAR(9)

DECLARE @totcntmm VARCHAR (MAX);
SELECT @totcntmm=CAST(COUNT(Report_Date) AS varchar) FROM dbo.#ResultsBloc_2
PRINT @totcntmm

DECLARE @body1 VARCHAR (MAX); 
		SET @body1 = 'Hi All,
		
Email Spam Page data SQL code executed for '+convert(varchar,(GETDATE()-1),102)+'.
		
Total count of call ids added are '+ (@totcntmm) +'. 

Regards,
Business Analytics';

print @body1
DECLARE @subject1 VARCHAR (MAX); 
SET @subject1 = 'Email Spam Page data SQL code executed for ' + convert(varchar,(GETDATE()-1),102);
print @subject1
	
   --send email
	if (SELECT count(Report_Date) FROM dbo.#ResultsBloc_2)>0
		EXEC msdb.dbo.sp_send_dbmail
		@profile_name = 'DW Mail',--@@SERVERNAME, --'DFW2-BISQL-001',
		@from_address ='_Group - Data Warehousing<dw@radiusgs.com>',
		@recipients = 'Darpan Thakkar <Darpan.Thakkar@radiusgs.com>; 
		Mukesh Salunke <Mukesh.Salunke@radiusgs.com>;',
		@copy_recipients='Pulkit.Jain@radiusgs.com;dw@radiusgs.com',
		@subject = @subject1,
		@body = @body1;
 END