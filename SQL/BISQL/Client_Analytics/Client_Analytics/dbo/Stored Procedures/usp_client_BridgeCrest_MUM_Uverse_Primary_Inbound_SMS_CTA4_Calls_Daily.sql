CREATE PROCEDURE [dbo].[usp_client_BridgeCrest_MUM_Uverse_Primary_Inbound_SMS_CTA4_Calls_Daily]

AS

BEGIN

	DECLARE 
	@startdate date = cast (getdate () -7 as date ),
	@enddate date = cast (getdate ()  as date )

-----Total No. of accounts for Bridgecrest
IF OBJECT_ID('tempdb..#act_prev') IS NOT NULL
DROP TABLE #act_prev;
SELECT cast(dcu.CustomerId as varchar(max))CustomerId
    , dcu.ClientAccountNumber
    , Concat(dcu.FirstName ,' ', dcu.LastName) AS Consumer_Name  
	,dcu.keysourcesystem
	, dcu.KeyCustomer
	, dcl.ClientParent
	, dcl.ClientId
	, dcl.ClientStreamId
	, dcl.ClientStream
	--, dcu.StatusCode
	, dcu.SourceSystem
	, dcu.ListDate
	, dcu.InitialBalance
	, dcu.CustomerState
INTO #act_prev
FROM [DW_MSTR_DM].[dbo].DimCustomer dcu WITH (NOLOCK)
		JOIN DW_MSTR_DM.dbo.DimClient dcl WITH (NOLOCK) 
		ON dcu.ClientId = dcl.ClientId
       AND dcu.sourcesystem = dcl.sourcesystem
WHERE dcl.ClientStreamId = 'IMBRIDGECR'
      AND dcu.keysourcesystem = 2


---Calls data for Service_Name 'MUM_Uverse Primary Inbound SMS CTA4'
IF OBJECT_ID('tempdb..#rcalls_raw') IS NOT NULL
	DROP TABLE #rcalls_raw;	      
	SELECT rc.*
		 , CASE WHEN rc.Account_Number like '%-%' then left(rc.Account_Number,charindex('-',rc.Account_Number)-1) else rc.Account_Number end AS [Account Number]
		 , a.ClientParent
		 , a.ClientId 
		 , a.ClientStreamId AS [Merge Group]
		 , a.ClientStream
	     , a.ListDate AS  [Placement Date]
	     , a.InitialBalance AS [Balance at time of SMS]
		 , ISNULL(a.CustomerState,'') AS CustomerState

	into #rcalls_raw
	FROM [DW_MSTR_DM].[dbo].[RadiusCall] rc WITH (NOLOCK)  
	LEFT JOIN #act_prev a 
	on CASE WHEN rc.Account_Number like '%-%' then left(rc.Account_Number,charindex('-',rc.Account_Number)-1) else rc.Account_Number end = cast(a.CustomerID as varchar(max))

	WHERE 
	  rc.Call_Date >= @startdate
	--AND rc.Call_Date < '2026-02-01'
	AND a.CustomerId IS NOT NULL
	AND rc.[Service_Name] = 'MUM_Uverse Primary Inbound SMS CTA4'

	--select * from CLIENT_ANALYTICS.dbo.[RPT_BrdgeCrest_Daily_Inbound]
INSERT INTO CLIENT_ANALYTICS.dbo.[RPT_BrdgeCrest_Daily_Inbound]
	SELECT
 rr.Call_Date
,rr.Call_Center_Name
,rr.Call_Center_Id
,rr.LV_Client_Name
,rr.[Service_Name]
,rr.Service_Type
,rr.Service_Id
,rr.Transaction_Type
,rr.Answer_Type
,rr.Session_Id
,rr.Phone_Dialed
,[Account Number]
,ISNULL(rr.First_Name, '') AS First_Name
,ISNULL(rr.Last_Name, '') AS Last_Name
,rr.Call_Connect_Time_CT
,rr.Call_End_Time
,rr.Call_Duration
,rr.IVR_Duration
,rr.Hold_Time
,rr.Transfer_Duration
,rr.Agent_Logon_Id
,rr.Agent_Full_Name
,rr.Agent_Team
,rr.Talk_Time
,rr.Wrap_Time
,rr.Agent_Hold_Time
,rr.Livevox_Result
,rr.Result_Code
,rr.Result_Id
,rr.Agent_Desktop_Outcome
,rr.Result_Category
,ISNULL(rr.Zip, '') AS Zip
,rr.Caller_Id
,rr.Phone_Number
,rr.[Data_Source]
,rr.Is_RPC
,rr.Is_Promise
,rr.[ClientID]
,rr.[Merge Group]
,rr.ClientStream
FROM #rcalls_raw rr
LEFT JOIN CLIENT_ANALYTICS.dbo.[RPT_BrdgeCrest_Daily_Inbound] rbdi
ON rr.Session_Id = rbdi.Session_Id
	WHERE rbdi.Session_Id IS NULL 
	--ORDER BY Call_Connect_Time_CT


	  DROP TABLE IF EXISTS ##final_result
	select * into ##final_result from CLIENT_ANALYTICS.dbo.[RPT_BrdgeCrest_Daily_Inbound]



	DECLARE @tab char(1) = CHAR(9);
	DECLARE @subject VARCHAR (MAX);
	DECLARE @body VARCHAR (MAX);
	declare @Attachments varchar(4000);
	
	--No of rows in the table
	DECLARE @callcount varchar(10) =(select  count(*) from  CLIENT_ANALYTICS.dbo.[RPT_BrdgeCrest_Daily_Inbound])
	--DECLARE @body1 VARCHAR(1000)

	SET @subject = 'BridgeCrest daily - MUM_Uverse Primary Inbound SMS CTA4 calls Report';
	--Query to get the current month data
	DECLARE @query VARCHAR (MAX); 
		SELECT  @query = 'SELECT Call_Date AS ''Call_Date'' 
		,Call_Center_Name AS ''Call_Center_Name'' 
		,Call_Center_Id AS ''Call_Center_Id'' 
		,LV_Client_Name AS ''LV_Client_Name'' 
		,Service_Name AS ''Service_Name'' 
		,Service_Type AS ''Service_Type'' 
		,Service_Id AS ''Service_Id'' 
		,Transaction_Type AS ''Transaction_Type'' 
		,Answer_Type AS ''Answer_Type'' 
		,Session_Id AS ''Session_Id'' 
		,Phone_Dialed AS ''Phone_Dialed'' 
		,Account_Number AS ''Account_Number'' 
		,First_Name AS ''First_Name'' 
		,Last_Name AS ''Last_Name'' 
		,Call_Connect_Time_CT  AS ''Call_Connect_Time_CT'' 
		,Call_End_Time  AS ''Call_End_Time'' 
		,Call_Duration AS ''Call_Duration'' 
		,IVR_Duration AS ''IVR_Duration'' 
		,Hold_Time AS ''Hold_Time'' 
		,Transfer_Duration AS ''Transfer_Duration'' 
		,Agent_Logon_Id AS ''Agent_Logon_Id'' 
		,Agent_Full_Name AS ''Agent_Full_Name'' 
		,Agent_Team AS ''Agent_Team'' 
		,Talk_Time AS ''Talk_Time'' 
		,Wrap_Time AS ''Wrap_Time'' 
		,Agent_Hold_Time AS ''Agent_Hold_Time'' 
		,Livevox_Result AS ''Livevox_Result'' 
		,Result_Code AS ''Result_Code'' 
		,Result_Id AS ''Result_Id'' 
		,Agent_Desktop_Outcome AS ''Agent_Desktop_Outcome'' 
		,Result_Category AS ''Result_Category'' 
		,Zip AS ''Zip'' 
		,Caller_Id AS ''Caller_Id'' 
		,Phone_Number AS ''Phone_Number'' 
		,Data_Source AS ''Data_Source'' 
		,Is_RPC AS ''Is_RPC'' 
		,Is_Promise AS ''Is_Promise'' 
		,ClientID AS ''ClientID'' 
		,[Merge Group] AS ''Merge Group'' 
		,ClientStream AS ''ClientStream''   
		 FROM CLIENT_ANALYTICS.dbo.[RPT_BrdgeCrest_Daily_Inbound]
		 ORDER BY Call_Connect_Time_CT';
		--SELECT CAST(Call_Date AS VARCHAR),cast(Call_Center_Name as varchar),CAST(Call_Center_Id AS VARCHAR),cast(LV_Client_Name as varchar),cast(Service_Name as varchar),cast(Service_Type as varchar),CAST(Service_Id AS VARCHAR),cast(Transaction_Type as varchar),cast(Answer_Type as varchar),CAST(Session_Id AS VARCHAR),cast(Phone_Dialed as varchar),CAST(Account_Number AS VARCHAR),cast(First_Name as varchar),cast(Last_Name as varchar),CONVERT(VARCHAR(23),cast(Call_Connect_Time_CT as datetime),121)  AS Call_Connect_Time_CT,CONVERT(VARCHAR(23), cast(Call_End_Time as datetime),121) ,CAST(Call_Duration AS VARCHAR),CAST(IVR_Duration AS VARCHAR),CAST(Hold_Time AS VARCHAR),CAST(Transfer_Duration AS VARCHAR),cast(Agent_Logon_Id as varchar),cast(Agent_Full_Name as varchar),cast(Agent_Team as varchar),CAST(Talk_Time AS VARCHAR),CAST(Wrap_Time AS VARCHAR),CAST(Agent_Hold_Time AS VARCHAR),cast(Livevox_Result as varchar),cast(Result_Code as varchar),CAST(Result_Id AS VARCHAR),cast(Agent_Desktop_Outcome as varchar),cast(Result_Category as varchar),cast(Zip as varchar),cast(Caller_Id as varchar),cast(Phone_Number as varchar),cast(Data_Source as varchar),CAST(Is_RPC AS VARCHAR),CAST(Is_Promise AS VARCHAR),CAST(ClientID AS VARCHAR),CAST([Merge Group] AS VARCHAR),cast (ClientStream as varchar) FROM CLIENT_ANALYTICS.dbo.[RPT_BrdgeCrest_Daily_Inbound]';
		PRINT @query
	SET @body =     'Hi Ted,' + CHAR(13) + CHAR(10) +
'
The number of calls for service MUM_Uverse Primary Inbound SMS CTA4 starting from 1st Jan 2026 are ' 
    + CAST(@callcount AS VARCHAR(20)) + '.' + CHAR(13) + CHAR(10) +
'
Please see attached .csv file for more details.' + CHAR(13) + CHAR(10) + CHAR(13) + CHAR(10) +
'
Thanks,
Data Warehousing Team';
		
		--send email
		if (SELECT count(Session_Id) FROM ##final_result)>0
			EXEC msdb.dbo.sp_send_dbmail
			@profile_name = 'DW Mail',--@@SERVERNAME, --'DFW2-BISQL-001',
			@from_address ='_Group - Data Warehousing <dw@radiusgs.com>',
			-- THE DISTRIBUTION LIST 
			@recipients = 'ted.miller@radiusgs.com;brett.leckerman@radiusgs.com',
			@copy_recipients='dw@radiusgs.com',
			--@recipients = 'amod.ramugade@radiusgs.com',
			--@copy_recipients ='amod.ramugade@radiusgs.com',
			@subject = @subject,
			@body = @body,
			@query = @query , 
			@execute_query_database ='CLIENT_ANALYTICS',
			@query_result_header = 1,
			@attach_query_result_as_file = 1
		   ,@query_attachment_filename = 'BridgeCrest Daily - MUM_Uverse Primary Inbound SMS CTA4.csv'
		   ,@query_result_separator = @tab
		   ,@query_result_no_padding = 1 
		   ,@query_result_width = 32767; 

END