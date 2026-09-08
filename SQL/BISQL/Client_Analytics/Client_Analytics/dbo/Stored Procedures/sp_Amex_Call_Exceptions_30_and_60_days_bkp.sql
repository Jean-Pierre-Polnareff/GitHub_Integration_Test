CREATE PROCEDURE [dbo].[sp_Amex_Call_Exceptions_30_and_60_days_bkp]
AS
BEGIN TRY
DECLARE @ErrorMessage VARCHAR(MAX) ;
DECLARE @subject1 VARCHAR(MAX);
DECLARE @body1 VARCHAR(MAX);
DECLARE @mmdd VARCHAR(MAX);
DECLARE @query1 VARCHAR(MAX);
DECLARE  @attach_query_result_as_file1 INT;
DECLARE @attachment VARCHAR(MAX);

SELECT * INTO  #TMP_query_result  FROM
(
SELECT c.ListDate AS Placement_Date,
 a.Call_Date
 , c.SourceSystem AS CRM
 , [Sub-Segment] = 'Digital First 30'
 ,c.ClientId AS RCode
  , c.CustomerId AS FileNumber
  , a.Agent_Logon_Id
  ,a.Agent_Full_Name
  , a.Agent_Team
 , a.Call_Connect_Time_CT
  , a.Call_End_Time
  ,a.Transaction_Type AS [Outbound/Inbound]
   , a.Livevox_Result
  , a.Phone_Dialed
  , a.Session_Id 
   
  FROM 
 dw_mstr_dm.[dbo].[vwCA_RadiusCall] (nolock) a
	JOIN DW_MSTR_DM.dbo.FactCustomerCall (nolock) b
	ON a.Session_Id = b.SessionId
	JOIN dw_mstr_dm.[dbo].[vwCA_DimCustomer] (NOLOCK) c
	ON b.KeyCustomer = c.KeyCustomer
	WHERE
		a.LV_Client_Name = 'Veldos'
		AND ClientId = '113HI1RG' 
		AND SourceSystem = 'AMEX Latitude'
AND c.ListDate <> '9999-12-31'
AND c.StatusCode <> 'dw_deactivate'
AND DATEADD(DAY, 30, c.ListDate)>= a.Call_Date
AND a.Call_Date = CAST(DATEADD(DAY, -1, GETDATE()) AS DATE)
AND a.Livevox_Result NOT IN ('SMS MT Failed', 'SMS MT Delivered')


UNION

SELECT c.ListDate AS Placement_Date,
 a.Call_Date
 , c.SourceSystem AS CRM
 ,[Sub-Segment] = 'Digital First 60'
 ,c.ClientId AS RCode
  , c.CustomerId AS FileNumber
  , a.Agent_Logon_Id
  ,a.Agent_Full_Name
  , a.Agent_Team
 , a.Call_Connect_Time_CT
  , a.Call_End_Time
  ,a.Transaction_Type AS [Outbound/Inbound]
   , a.Livevox_Result
  , a.Phone_Dialed
  , a.Session_Id 
   
  FROM 
 dw_mstr_dm.[dbo].[vwCA_RadiusCall] (nolock) a
	JOIN DW_MSTR_DM.dbo.FactCustomerCall (nolock) b
	ON a.Session_Id = b.SessionId
	JOIN dw_mstr_dm.[dbo].[vwCA_DimCustomer] (NOLOCK) c
	ON b.KeyCustomer = c.KeyCustomer
	WHERE
		a.LV_Client_Name = 'Veldos'
		AND ClientId = '113HI2RG' 
		AND SourceSystem = 'AMEX Latitude'
AND c.ListDate <> '9999-12-31'
AND c.StatusCode <> 'dw_deactivate'
AND DATEADD(DAY, 60, c.ListDate)>= a.Call_Date
AND a.Call_Date = CAST(DATEADD(DAY, -1, GETDATE()) AS DATE)
AND a.Livevox_Result NOT IN ('SMS MT Failed', 'SMS MT Delivered')

) AS A1;



IF (SELECT COUNT(*) AS No_of_Rows FROM #TMP_query_result ) > 0 
SET @attach_query_result_as_file1 = 1
ELSE
SET @attach_query_result_as_file1 = 0;
SET @attachment = 'Amex_Call_Exceptions_' + convert(varchar(8),cast(GETDATE() -1  as date),112) + '.csv';
SET @mmdd =  RIGHT(CONVERT(VARCHAR ,GETDATE()-1, 111), 5);
SET @subject1 = 'Amex Call Exceptions for 113HI1RG and 113HI2RG on ' + @mmdd;

IF (SELECT COUNT(*) AS No_of_Rows FROM #TMP_query_result ) > 0 
SET @body1 = 'Hi Bilal, 

Please see attached the Amex Call Exceptions for 113HI1RG and 113HI2RG on ' + @mmdd + '.';
ELSE
SET @body1 = 'Hi Bilal,

There were no exceptions found for 113HI1RG and 113HI2RG on ' + @mmdd + '.';

IF (SELECT COUNT(*) AS No_of_Rows FROM #TMP_query_result ) > 0 
SET @query1 = 'SET NOCOUNT ON; 
	SELECT ''Placement_Date'',	''Call_Date'',	''CRM'',	''Sub-Segment'',	''RCode'',	''FileNumber'',	''Agent_Logon_Id'',	''Agent_Full_Name'',
	''Agent_Team'',	''Call_Connect_Time_CT'',	''Call_End_Time'',	''Outbound/Inbound'',	''Livevox_Result'',		''Phone_Dialed'',	''Session_Id''
	UNION 
	SELECT 
	CAST(c.ListDate AS VARCHAR)    
  , CAST(a.Call_Date AS VARCHAR)
  , CAST(c.SourceSystem AS VARCHAR) 
  , [Sub-Segment] = ''Digital First 30''
  , CAST(c.ClientId AS VARCHAR) 
  , CAST(c.CustomerId AS VARCHAR) 
  , CAST(a.Agent_Logon_Id AS VARCHAR)
  , CAST(a.Agent_Full_Name AS VARCHAR)
  , CAST(a.Agent_Team AS VARCHAR)
  , CAST(a.Call_Connect_Time_CT AS VARCHAR)
  , CAST(a.Call_End_Time AS VARCHAR)
  , CAST(a.Transaction_Type AS VARCHAR) 
  , CAST(a.Livevox_Result AS VARCHAR)
  , CAST(a.Phone_Dialed AS VARCHAR)
  , CAST(a.Session_Id AS VARCHAR)
   
  FROM 
  dw_mstr_dm.[dbo].[vwCA_RadiusCall] (nolock) a
  JOIN DW_MSTR_DM.dbo.FactCustomerCall (nolock) b
  ON a.Session_Id = b.SessionId
  JOIN dw_mstr_dm.[dbo].[vwCA_DimCustomer] (NOLOCK) c
  ON b.KeyCustomer = c.KeyCustomer
  WHERE

  a.LV_Client_Name = ''Veldos''
  AND ClientId = ''113HI1RG'' 
  AND SourceSystem = ''AMEX Latitude''
  AND c.ListDate <> ''9999-12-31''
  AND c.StatusCode <> ''dw_deactivate''
  AND DATEADD(DAY, 30, c.ListDate)>= a.Call_Date
  AND a.Call_Date = CAST(DATEADD(DAY, -1, GETDATE()) AS DATE)
  AND a.Livevox_Result NOT IN (''SMS MT Failed'', ''SMS MT Delivered'')
 
 UNION
  
    SELECT 
    CAST(c.ListDate AS VARCHAR) 
  , CAST(a.Call_Date AS VARCHAR)
  , CAST(c.SourceSystem AS VARCHAR) 
  , [Sub-Segment] = ''Digital First 60''
  , CAST(c.ClientId AS VARCHAR) 
  , CAST(c.CustomerId AS VARCHAR) 
  , CAST(a.Agent_Logon_Id AS VARCHAR)
  , CAST(a.Agent_Full_Name AS VARCHAR)
  , CAST(a.Agent_Team AS VARCHAR)
  , CAST(a.Call_Connect_Time_CT AS VARCHAR)
  , CAST(a.Call_End_Time AS VARCHAR)
  , CAST(a.Transaction_Type AS VARCHAR) 
  , CAST(a.Livevox_Result AS VARCHAR)
  , CAST(a.Phone_Dialed AS VARCHAR)
  , CAST(a.Session_Id AS VARCHAR) 
     
  FROM 
  dw_mstr_dm.[dbo].[vwCA_RadiusCall] (nolock) a
  JOIN DW_MSTR_DM.dbo.FactCustomerCall (nolock) b
  ON a.Session_Id = b.SessionId
  JOIN dw_mstr_dm.[dbo].[vwCA_DimCustomer] (NOLOCK) c
  ON b.KeyCustomer = c.KeyCustomer
  WHERE
  a.LV_Client_Name = ''Veldos''
  AND ClientId = ''113HI2RG'' 
  AND SourceSystem = ''AMEX Latitude''
  AND c.ListDate <> ''9999-12-31''
  AND c.StatusCode <> ''dw_deactivate''
  AND DATEADD(DAY, 60, c.ListDate)>= a.Call_Date
  AND a.Call_Date = CAST(DATEADD(DAY, -1, GETDATE()) AS DATE)
  AND a.Livevox_Result NOT IN (''SMS MT Failed'', ''SMS MT Delivered'')
  ORDER BY 2 DESC, 4 ASC' ; 
ELSE

SET @query1 = 'SET NOCOUNT ON; 
	SELECT ''''';

EXEC msdb.dbo.sp_send_dbmail
    @profile_name = 'DW Mail',
	@from_address ='dw@radiusgs.com',
    @recipients = 'amod.ramugade@radiusgs.com', 
	@copy_recipients = 'amod.ramugade@radiusgs.com',
	@subject = @subject1,
    @body = @body1,
	
	@query =   @query1,
    
  @attach_query_result_as_file = @attach_query_result_as_file1,
  @query_result_header = 0,
  @query_result_width = 32767,
  @query_result_separator = '	',
  @exclude_query_output = 1,
  @append_query_error = 0,
  @query_no_truncate = 0,
  @query_result_no_padding = 1,	   
  @query_attachment_filename = @attachment;
  
	
	END TRY
BEGIN CATCH
    SELECT @ErrorMessage = @@SERVERNAME + '.' + DB_NAME() + '..' + OBJECT_NAME(@@PROCID) + ': ' + ERROR_MESSAGE();
	RAISERROR(@ErrorMessage,16,1);
END CATCH;