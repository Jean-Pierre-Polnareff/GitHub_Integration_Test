
CREATE PROCEDURE [dbo].[usp_insert_RPT_client_CAM_digital]
		

AS
/* 
Object: [usp_insert_RPT_client_CAM_digital]

Description: Insert last Month's digital metrics for Crown Asset Management(CAM) and send the report via SFTP in .csv file

Author			Date		Description
Amod Ramugade	02/25/2026	Created
*/

BEGIN
	SET NOCOUNT ON;

DECLARE @startdate date =  CAST(DATEADD(DAY,-DATEPART(DAY,DATEADD(MONTH,-1,GETDATE()))+1,DATEADD(MONTH,-1,GETDATE())) AS DATE)  --first day of last mo

DECLARE  @enddate date =   DATEADD(DAY,-DATEPART(DAY,GETDATE())+1,CAST(GETDATE() AS DATE))  --first day of curr mo;

DECLARE @monthname varchar(20)=datename(month,@startdate)

DECLARE @year varchar(20)=cast(year(@startdate) as varchar)
 
DECLARE @DataFile VARCHAR(255) = 'Digital_' + CONVERT(CHAR(8), @startdate, 112) + '.csv'

PRINT @datafile

DROP TABLE IF EXISTS dbo.#Results1
SELECT ARACID AS CustomerID, ARACCLACCT as ORNGLACC
, ZZACCREDINTCONSACNUM AS CreditorInternalConsumerAccountNumber
INTO dbo.#Results1
FROM OPENQUERY([THIRDPROD],'
SELECT  account.ARACID, account.ARACCLACCT, account.ZZACCREDINTCONSACNUM
FROM ARCLIENT clientinfo 
	INNER JOIN SQLUser.ARACCOUNT account ON clientinfo.ARCLID = account.ARACCLTID
	where clientinfo.ZZCLATTRMERGINGGRP=''IMCROWN''')

--	SELECT * FROM #Results1 where CreditorInternalConsumerAccountNumber = '1902P002602068'


	DROP TABLE IF EXISTS #t1
		SELECT  dcu.*, dcl.ClientParent, dcl.ClientStream, dcl.ClientStreamId into #t1
		 FROM DW_MSTR_DM.dbo.dimcustomer (nolock) dcu 
		 join DW_MSTR_DM.dbo.DimClient dcl on dcu.KeyClient = dcl.KeyClient 
		 and dcl.ClientStreamId = 'IMCROWN'		


		IF OBJECT_ID('tempdb..#emails') IS NOT NULL
			DROP TABLE #emails;
 
		SELECT    
		         r.CreditorInternalConsumerAccountNumber   AS CrownID
				--,'`' + CAST(r.ORNGLACC as varchar(255))  AS CrownID
		        , em.KeyCustomer
		        , em.FileNumber
				, em.[GUID] as SessionId
				, em.EventDate
				, dd2.CalendarDate as email_sent_date
				, 'Email' as attempt_type
				, EmailAddress
				, EventID
				, EventValue
				, LTR
			    , case when EventID = 10 then 'Delivered'
		               when EventID = 8 and em.[Description] in ('Hard-Bounce','Hard Bounce') then 'Failed' 
		               when EventID = 8 
					        and em.eventvalue in ('INVALID_EMAIL','Blocked','BOUNCE') 
					   	 and em.vendor ='Revspring' 
					        then 'Failed'
				      when EventID = 5 then 'Opened'
				      when EventID = 12 then 'Clicked'
				      when EventID = 6 then 'Unsubscribed'
					  else 'Other' End Result
				, em.EventCount
				, a.CustomerCity
				, a.CustomerState
				, ISNULL(CASE WHEN LEN(a.CustomerZip) > 5 THEN LEFT(a.CustomerZip,5) ELSE a.CustomerZip END , '') AS CustomerZip
			
		INTO #emails
		FROM 
			 DW_MSTR_DM.dbo.Radius_EmailReportData em (NOLOCK)
		JOIN DW_MSTR_DM.dbo.Dimclient dcl (NOLOCK) on dcl.clientid = em.clientsclientid and dcl.ClientStreamId = 'IMCROWN'
				LEFT JOIN
			 DW_MSTR_DM.dbo.DimDate dd2 (NOLOCK) ON cast(em.EventDate as date)=dd2.CalendarDate
			 LEFT JOIN #Results1 r ON r.CustomerID = em.FileNumber
			 LEFT JOIN #t1 a ON em.FileNumber = a.CustomerId
		WHERE em.EventID in (8,10,5,12,6) and em.clientsclientid <> 'Warmup Test'
		and dd2.CalendarDate >= @startdate and dd2.CalendarDate < @enddate

--		SELECT * FROM #emails

DROP TABLE IF EXISTS  #SMS_results;
SELECT 'Consumer responded Help to text before Authentication' AS Dialer_Result,	'Delivered' AS Alternative_Result, 	'IN' AS Direction
INTO #SMS_results
UNION SELECT 'Consumer responded Stop to text',	'Unsubscribed',	'IN'
UNION SELECT 'SMS MT Delivered',	'Delivered',	'OUT'
UNION SELECT 'SMS MT Failed',	'Failed',	'OUT'
UNION SELECT 'SMS Text Sent and authenticated',	'Delivered',	'OUT'
UNION SELECT 'SMS MO Received',	'Delivered',	'IN'


	IF OBJECT_ID('tempdb..#rcalls_raw') IS NOT NULL
	DROP TABLE #rcalls_raw;	      
	SELECT 
	      r.CreditorInternalConsumerAccountNumber AS CrownID
		--'`' + CAST(r.ORNGLACC as varchar(255))  AS CrownID
	     , rc.session_Id
		 , rc.livevox_result
		-- , rc.[Service_Id]
		 , rc.[Service_Name]
		 --, rc.Is_RPC 
		 , CASE WHEN rc.Account_Number like '%-%' then left(rc.Account_Number,charindex('-',rc.Account_Number)-1) else rc.Account_Number end AS [Account Number]	 
		 --, rc.[File_Name] AS Campaign
		 --, a.ClientParent
		 , a.ClientId AS [Practice Code]
		 , a.ClientStreamId as [Merge Group]
		 , a.ClientStream
	     , a.ListDate AS  [Placement Date]
	     , a.InitialBalance AS [Balance at time of SMS]
	     , rc.Call_Date  AS [Date of SMS]
         , rc.Call_Connect_Time_CT
		 , Letter_Code = ISNULL(rc.Letter_Code,'')
		 , CallerPhoneNumber = ISNULL(rc.Phone_Number,'')
		 , DialedPhoneNumber = ISNULL(rc.Phone_Dialed,'')
		 , a.CustomerCity
		 , a.CustomerState
		 , ISNULL(CASE WHEN LEN(a.CustomerZip) > 5 THEN LEFT(a.CustomerZip,5) ELSE a.CustomerZip END , '') AS CustomerZip

	into #rcalls_raw
	FROM [DW_MSTR_DM].[dbo].[RadiusCall] rc WITH (NOLOCK)  
	LEFT JOIN #t1 a 
	on CASE WHEN rc.Account_Number like '%-%' then left(rc.Account_Number,charindex('-',rc.Account_Number)-1) else rc.Account_Number end = cast(a.CustomerID as varchar(max))
			 LEFT OUTER JOIN 
		 DW_MSTR_DM.dbo.TimeZoneByState T (NOLOCK) ON a.CustomerState = T.State_Abbr
		 LEFT JOIN #Results1 r 
	on CASE WHEN rc.Account_Number like '%-%' then left(rc.Account_Number,charindex('-',rc.Account_Number)-1) else rc.Account_Number end = cast(r.CustomerID as varchar(max))

	WHERE 
	--rc.Call_Date >=  '07-01-2024'
	  rc.Call_Date>=  @startdate
	AND rc.Call_Date < @enddate
	AND (rc.[livevox_result] like 'SMS%' or rc.[livevox_result] like '%text%')
    --AND rc.Creditor_Code=a.ClientId
	AND a.CustomerId IS NOT NULL
		--AND a.listdate >= '2025-01-01'

/*
		SELECT * FROM #rcalls_raw --64274
	SELECT  Service_Name, livevox_result, count(*) FROM #rcalls_raw group by  Service_Name , livevox_result order by Service_Name
*/

	 TRUNCATE TABLE CLIENT_ANALYTICS.dbo.Client_CAM_Monthly_Email_SMS_Log 

	 INSERT INTO CLIENT_ANALYTICS.dbo.Client_CAM_Monthly_Email_SMS_Log 
	 (
	 [CrownID]
      ,[CommID]
      ,[DateTime]
      ,[Direction]
      ,[Category]
      ,[Sub Category]
      ,[User]
      ,[UserType]
      ,[Email Address]
      ,[Phone Number]
      ,[Content]
      ,[Language]
      ,[Result]
      ,[Consent]
      ,[Notes]
	  ,[City]
	  ,[State]
	  ,[ZIP]
	  )

	 	 SELECT 
	   [CrownID]
	 --, [CreditorInternalConsumerAccountNumber] AS [CommID]
	 , [CommID] = ''
	 , [EventDate] AS [DateTime]
	 , [Direction] = CASE WHEN Result IN ('Unsubscribed') THEN 'IN' ELSE  'OUT' END
	 , attempt_type AS [Category] 
	 , [LTR] AS [Sub Category]
	 , [User] = ''
	 , [UserType] = 'User'
	 , [EmailAddress] AS [Email Address]
     , [Phone Number] = ''
     , [Content] = ''
     , [Language] = 'EN'
     , [Result]
     , [Consent] = ''
     , [Notes] = ''
	 , [CustomerCity] AS [City]
	 , [CustomerState] AS [State]
	 , [CustomerZip] AS [ZIP]
	 FROM  #emails 
	 
	 --ORDER BY [DateTime]

UNION 

	 SELECT 
	   [CrownID]
	 --, [CreditorInternalConsumerAccountNumber] AS [CommID]
	 , [CommID] = ''
	 , [Call_Connect_Time_CT] AS [DateTime]
	 , [Direction] = sr.Direction
	 --, [Direction] = CASE WHEN Livevox_Result IN ('SMS MT Failed', 'SMS MT Delivered','SMS Text Sent and authenticated') THEN 'OUT' ELSE 'IN' END
	 , [Category] = CASE WHEN [Service_Name] LIKE '%MMS%' THEN 'MMS' ELSE 'SMS' END
	 , [Letter_Code] AS [Sub Category]
	 , [User] = ''
	 , [UserType] = 'User'
	 , [Email Address] = ''
	 , [Phone Number] = CASE WHEN sr.Direction = 'OUT' THEN DialedPhoneNumber ELSE CallerPhoneNumber END
     --, [Phone Number] = CASE WHEN Livevox_Result IN ('SMS MT Failed', 'SMS MT Delivered','SMS Text Sent and authenticated') THEN DialedPhoneNumber ELSE CallerPhoneNumber END
     , [Content] = ''
     , [Language] = 'EN'
	 , [Result] = sr.Alternative_Result
     --, [Livevox_Result] AS [Result]
     , [Consent] = ''
     , [Notes] = ''
	 , [CustomerCity] AS [City]
	 , [CustomerState] AS [State]
	 , [CustomerZip] AS [ZIP]

	 FROM  #rcalls_raw rr 
	    left join #SMS_results sr 
	    ON rr.Livevox_Result = sr.Dialer_Result
	 
	 --ORDER BY [DateTime]


	  BEGIN TRY
DECLARE @ErrorMessage VARCHAR(MAX) ;
DECLARE @subject1 VARCHAR(MAX);
DECLARE @body1 VARCHAR(MAX);
DECLARE @mmyy VARCHAR(MAX);
DECLARE @attachment VARCHAR(MAX);

SET NOCOUNT ON;

--Extract data into CSV file
DECLARE 
@vFeed       VARCHAR(1000) =  '\\dfw2-bisql-001\SSISFlatFileStage\ClientExport\CrownAssetDigitalReport\Queue\'  +  @datafile,
--@vZipped      VARCHAR(1000) =  LEFT(@datafile,16) + '.zip',
@vBCP        VARCHAR(8000) = 'SET NOCOUNT ON; SELECT * FROM CLIENT_ANALYTICS.dbo.vw_CAM_Digital_Report ORDER BY 7 DESC'

SET @vBCP = 'bcp "' + @vBCP + '" queryout "' + @vFeed + '" -c -t, -T';
EXEC xp_cmdshell @vBCP, no_output;

/*
-- Compress the CSV file
SET @vBCP = 'call C:\"Program Files"\7-Zip\7z a -tzip \\dfw2-bisql-001\SSISFlatFileStage\ClientExport\CrownAssetDigitalReport\Zipped\' + @vZipped + ' ' + @vFeed;
EXEC xp_cmdshell @vBCP, no_output;

--FTP the zipped file
SET @vBCP = '\\dfw2-bisql-001\SSISFlatFileStage\ClientExport\CrownAssetDigitalReport\WinSCP\WinSCP.com /ini=nul /command "open sftp://CLI6520-COM@transfer.CROWNASSET.COM/ -hostkey=""ssh-rsa 2048 cexRNrAvsB2MPxMsbUXqSETutEmF3ENh0Xpoim8QJmQ="" -privatekey=\\dfw2-adman-001\e\TEMP\CAM\CAM_SSH_Key.ppk" "lcd \\dfw2-bisql-001\SSISFlatFileStage\ClientExport\CrownAssetDigitalReport\Zipped\" "cd /deliverables" "put -resumesupport=off ' + @vZipped + '" "exit"';
EXEC xp_cmdshell @vBCP, no_output;

-- Move file into Archive folder
SET @vBCP = 'move /Y \\dfw2-bisql-001\SSISFlatFileStage\ClientExport\CrownAssetDigitalReport\Zipped\' + @vZipped + ' \\dfw2-bisql-001\SSISFlatFileStage\ClientExport\CrownAssetDigitalReport\Archive\';
EXEC xp_cmdshell @vBCP, no_output;
*/

--FTP the csv file
SET @vBCP = '\\dfw2-bisql-001\SSISFlatFileStage\ClientExport\CrownAssetDigitalReport\WinSCP\WinSCP.com /ini=nul /command "open sftp://CLI6520-COM@transfer.CROWNASSET.COM/ -hostkey=""ssh-rsa 2048 cexRNrAvsB2MPxMsbUXqSETutEmF3ENh0Xpoim8QJmQ="" -privatekey=\\dfw2-adman-001\e\TEMP\CAM\CAM_SSH_Key.ppk" "lcd \\dfw2-bisql-001\SSISFlatFileStage\ClientExport\CrownAssetDigitalReport\Queue\" "cd /deliverables" "put -resumesupport=off ' + @datafile + '" "exit"';
EXEC xp_cmdshell @vBCP, no_output;

-- Move file into Archive folder
SET @vBCP = 'move /Y \\dfw2-bisql-001\SSISFlatFileStage\ClientExport\CrownAssetDigitalReport\Queue\' + @datafile + ' \\dfw2-bisql-001\SSISFlatFileStage\ClientExport\CrownAssetDigitalReport\Archive\';
EXEC xp_cmdshell @vBCP, no_output;

----Notify end users 
SET @mmyy =    FORMAT(DATEADD(month,  DATEDIFF(month, 0, GETDATE() ) , -31),'MMMM yyyy') ;
              
SET @subject1 =  'Crown Asset Digital Report for ' + @mmyy;


SET @body1 = 'Hi, 

The Crown Asset Digital Report for ' + @mmyy + ' named ' + '"' +@datafile + '"' + ' has been uploaded on SFTP server. ' 
  + 
'

Kindly reach out to analytics@radiusgs.com if any issues or questions.

Thank you';

EXEC msdb.dbo.sp_send_dbmail
    @profile_name = 'DW Mail',
	@from_address ='dw@radiusgs.com',
    @recipients = 'Salena.Arenivas@radiusgs.com; ClientServices-All@radiusgs.com', 
	@copy_recipients = 'dw@radiusgs.com; Amod.Ramugade@radiusgs.com',
	@subject = @subject1,
    @body = @body1;
	
END TRY
BEGIN CATCH
    SELECT @ErrorMessage = @@SERVERNAME + '.' + DB_NAME() + '..' + OBJECT_NAME(@@PROCID) + ': ' + ERROR_MESSAGE();
	RAISERROR(@ErrorMessage,16,1);
END CATCH;
	
END;		