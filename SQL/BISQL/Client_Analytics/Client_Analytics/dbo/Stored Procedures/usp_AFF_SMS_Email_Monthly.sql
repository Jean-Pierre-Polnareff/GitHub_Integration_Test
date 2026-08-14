
create proc [dbo].[usp_AFF_SMS_Email_Monthly]
as
begin
/*-----------------------------------------------------------
  1. SET DATE RANGE (LAST MONTH)
------------------------------------------------------------*/
 DECLARE @startdate date =  CAST(DATEADD(DAY,-DATEPART(DAY,DATEADD(MONTH,-1,GETDATE()))+1,DATEADD(MONTH,-1,GETDATE())) AS DATE)  --first day of last mo

 
DECLARE  @enddate date =   DATEADD(DAY,-DATEPART(DAY,GETDATE())+1,CAST(GETDATE() AS DATE))  --first day of curr mo;
 

-- print @startdate
-- print @enddate
/*-----------------------------------------------------------
  2. LOAD SMS DATA
------------------------------------------------------------*/
DROP TABLE IF EXISTS #t;
SELECT dcu.keycustomer,
       dcl.ClientId,
       dcl.ClientStreamId,
       dcl.ClientParent
INTO #t
FROM DW_MSTR_DM.dbo.dimcustomer dcu WITH(NOLOCK)
JOIN DW_MSTR_DM.dbo.dimclient dcl WITH(NOLOCK)
     ON dcl.keyclient = dcu.keyclient
    AND dcl.ClientStreamId IN ('IMAMER1FIN','IMAMER2FIN');
 
DROP TABLE IF EXISTS #SMS;
 
SELECT  
	  a.ClientId,      
      COUNT(*) AS Total_SMS_Sent,
      SUM(CASE WHEN ddr.dialer_result = 'SMS MO Received' THEN 1 ELSE 0 END) AS SMS_MO_Received,
      SUM(CASE WHEN ddr.dialer_result = 'SMS MT Failed' THEN 1 ELSE 0 END) AS SMS_Failed,
      SUM(CASE WHEN ddr.Dialer_Result = 'SMS MT Delivered' THEN 1 ELSE 0 END) AS SMS_Delivered
INTO #SMS
FROM DW_MSTR_DM.dbo.FactCustomerCall fcc WITH(NOLOCK)
JOIN #t a ON fcc.keycustomer = a.keycustomer
JOIN dw_mstr_dm.dbo.dimdialerresult ddr WITH(NOLOCK)
     ON ddr.KeyDialerResult = fcc.KeyDialerResult
WHERE ddr.Dialer_Result LIKE 'SMS%'
  AND CAST(fcc.CallStartTime AS DATE) >= @startdate
  AND CAST(fcc.CallStartTime AS DATE) < @enddate
GROUP BY a.ClientId;
 
/* SMS OPT-OUTS */
DROP TABLE IF EXISTS #SMS_OPT;
 
SELECT 
       a.ClientId
     , count(*) SMS_Optouts 
INTO #SMS_OPT
FROM CLIENT_ANALYTICS.dbo.RPT_SMS_Optout_detail r WITH(NOLOCK)
JOIN DW_MSTR_DM.dbo.DimClient a WITH(NOLOCK)
     ON r.ClientId = a.ClientId
WHERE a.ClientStreamId IN ('IMAMER1FIN','IMAMER2FIN')
  AND r.calldate >= @startdate
  AND r.calldate < @enddate
GROUP BY  a.ClientId;
 
 
/*-----------------------------------------------------------
  3. LOAD EMAIL DATA
------------------------------------------------------------*/
 
/* EMAIL SENDS */
DROP TABLE IF EXISTS #email_send;
 
SELECT
      dcl.ClientId,
      SUM(CASE WHEN EventID = 10 THEN EventCount ELSE 0 END) AS Email_Delivered,
      SUM(CASE WHEN EventID = 8  
                 AND emrd.[Description] IN ('Hard-Bounce','Hard Bounce')
               THEN 1
               WHEN EventID = 8 
                 AND emrd.EventValue IN ('INVALID_EMAIL','Blocked','BOUNCE')
                 AND emrd.Vendor='Revspring'
               THEN 1
               ELSE 0 END) AS Email_Bounced,
      COUNT(*) AS Email_Sent
INTO #email_send
FROM dw_mstr_dm.dbo.Radius_EmailReportData emrd WITH(NOLOCK)
JOIN DW_MSTR_DM.dbo.DimClient dcl WITH(NOLOCK)
     ON emrd.ClientsClientId = dcl.ClientId
WHERE dcl.ClientStreamId IN ('IMAMER1FIN','IMAMER2FIN')
  AND emrd.EventDate >= @startdate
  AND emrd.EventDate < @enddate
GROUP BY dcl.ClientId;
 
 
/* EMAIL OPENS */
DROP TABLE IF EXISTS #email_open;
 
SELECT 
      dcl.ClientId,
      SUM(emrd.EventCount) AS Email_Opened
INTO #email_open
FROM dw_mstr_dm.dbo.Radius_EmailReportData emrd WITH(NOLOCK)
JOIN DW_MSTR_DM.dbo.DimClient dcl WITH(NOLOCK)
     ON emrd.ClientsClientId = dcl.ClientId
WHERE EventID = 5
  AND dcl.ClientStreamId IN ('IMAMER1FIN','IMAMER2FIN')
  AND emrd.EventDate >= @startdate
  AND emrd.EventDate < @enddate
GROUP BY dcl.ClientId;
 
 
/* EMAIL CLICKS */
DROP TABLE IF EXISTS #email_click;
 
SELECT 
      dcl.ClientId,
      SUM(emrd.EventCount) AS Email_Clicked
INTO #email_click
FROM dw_mstr_dm.dbo.Radius_EmailReportData emrd WITH(NOLOCK)
JOIN DW_MSTR_DM.dbo.DimClient dcl WITH(NOLOCK)
     ON emrd.ClientsClientId = dcl.ClientId
WHERE EventID = 12
  AND dcl.ClientStreamId IN ('IMAMER1FIN','IMAMER2FIN')
  AND emrd.EventDate >= @startdate
  AND emrd.EventDate < @enddate
GROUP BY dcl.ClientId;
 
 
/* EMAIL OPTOUTS */
DROP TABLE IF EXISTS #email_opt;
 
SELECT 
      dcl.ClientId,
      SUM(emrd.EventCount) AS Email_Optouts
INTO #email_opt
FROM dw_mstr_dm.dbo.Radius_EmailReportData emrd WITH(NOLOCK)
JOIN DW_MSTR_DM.dbo.DimClient dcl WITH(NOLOCK)
     ON emrd.ClientsClientId = dcl.ClientId
WHERE EventID = 6
  AND dcl.ClientStreamId IN ('IMAMER1FIN','IMAMER2FIN')
  AND emrd.EventDate >= @startdate
  AND emrd.EventDate < @enddate
GROUP BY dcl.ClientId;
 
 
/*-----------------------------------------------------------
  4. FINAL MERGED OUTPUT
------------------------------------------------------------*/
DROP TABLE IF EXISTS ##FINAL;
 
SELECT 
      FORMAT(@startdate, 'yy-MMM', 'en-US') AS [Month-Year],

      c.ClientId,
      ISNULL(e.Email_Sent,0) AS Email_Sent,
      ISNULL(e.Email_Delivered,0) AS Email_Delivered,
      ISNULL(e.Email_Bounced,0) AS Email_Bounced,
      ISNULL(o.Email_Opened,0) AS Email_Opened,
      ISNULL(cl.Email_Clicked,0) AS Email_Clicked,
      ISNULL(op.Email_Optouts,0) AS Email_Optouts,
      ISNULL(s.Total_SMS_Sent,0) AS SMS_Total_Sent,
      ISNULL(s.SMS_Delivered,0) AS SMS_Total_Delivered,
      ISNULL(so.SMS_Optouts,0) AS SMS_Total_Optouts
INTO  ##FINAL
FROM DW_MSTR_DM.dbo.DimClient c WITH(NOLOCK)
LEFT JOIN #email_send e ON c.ClientId = e.ClientId
LEFT JOIN #email_open o ON c.ClientId = o.ClientId
LEFT JOIN #email_click cl ON c.ClientId = cl.ClientId
LEFT JOIN #email_opt op ON c.ClientId = op.ClientId
LEFT JOIN #SMS s ON c.ClientId = s.ClientId
LEFT JOIN #SMS_OPT so ON c.ClientId = so.ClientId
WHERE c.ClientStreamId IN ('IMAMER1FIN','IMAMER2FIN');



deCLARE @EmailDataExists INT = 0;
DECLARE @SMSDataExists INT = 0;

IF EXISTS (SELECT 1 FROM ##Final WHERE Email_Sent <> 0) SET @EmailDataExists = 1;
IF EXISTS (SELECT 1 FROM ##Final WHERE SMS_Total_Sent <> 0) SET @SMSDataExists = 1;

------------------------------------------------------------
-- 3️⃣ SEND ALERTS IF NO DATA
------------------------------------------------------------
IF @EmailDataExists = 0
BEGIN
    EXEC msdb.dbo.sp_send_dbmail
        @profile_name = @@SERVERNAME,
        @recipients = 'audumber.ghodake@radiusgs.com',
        @subject = 'Email Data Missing - AFF ',
        @body = 'No email data found for the report period.';
END

IF @SMSDataExists = 0
BEGIN
    EXEC msdb.dbo.sp_send_dbmail
        @profile_name = @@SERVERNAME,
        @recipients = 'audumber.ghodake@radiusgs.com',
        @subject = 'SMS Data Missing - AFF',
        @body = 'No SMS data found for the report period.';
END


------------------------------------------------------------
-- 4️⃣ EXPORT CSV IF DATA EXISTS
------------------------------------------------------------
IF (@EmailDataExists = 1 OR @SMSDataExists = 1)
BEGIN
    -- Create a global temp table to avoid BCP visibility issues
    IF OBJECT_ID('tempdb..##FinalExport') IS NOT NULL DROP TABLE ##FinalExport;

    SELECT 
        [Month-Year] AS Month_Year,
        ClientID,
        Email_Sent,
        Email_Delivered,
        Email_Bounced,
        Email_Opened,
        Email_Clicked,
        Email_Optouts,
        SMS_Total_Sent,
        SMS_Total_Delivered,
        SMS_Total_Optouts
    INTO ##FinalExport
    FROM ##Final;


    -- Destination file path
    DECLARE @DestinationFilePath VARCHAR(1000) = '\\dfw2-rgsfs-002.rgs.radiusgs.com\AFF\AFF_Email_SMS_' 
                                                   + DATENAME(month, @startdate) + CAST(YEAR(@startdate) AS VARCHAR(4))
  
    + '.csv';

    -- Use BCP to Export Data with Headers
    DECLARE @vBCPData VARCHAR(8000);
    SET @vBCPData = 'bcp "select ''Month_Year'' Month_Year, ''ClientID'' ClientID, ''Email_Sent'' Email_Sent, ''Email_Delivered'' Email_Delivered, ''Email_Bounced'' Email_Bounced, ''Email_Opened'' Email_Opened, ''Email_Clicked'' Email_Clicked, ''Email_Optouts'' Email_Optouts, ''SMS_Total_Sent'' SMS_Total_Sent, ''SMS_Total_Delivered'' SMS_Total_Delivered, ''SMS_Total_Optouts'' SMS_Total_Optouts union all select CAST(Month_Year AS VARCHAR), CAST(ClientID AS VARCHAR), CAST(Email_Sent AS VARCHAR), CAST(Email_Delivered AS VARCHAR), CAST(Email_Bounced AS VARCHAR), CAST(Email_Opened AS VARCHAR), CAST(Email_Clicked AS VARCHAR), CAST(Email_Optouts AS VARCHAR), CAST(SMS_Total_Sent AS VARCHAR), CAST(SMS_Total_Delivered AS VARCHAR), CAST(SMS_Total_Optouts AS VARCHAR) from ##FinalExport" queryout "' 
                  + @DestinationFilePath + '" -c -t, -T -S ' + @@SERVERNAME;

    EXEC xp_cmdshell @vBCPData;

    -- Cleanup global temp table
    DROP TABLE IF EXISTS ##FinalExport;
END
ELSE
BEGIN
    PRINT 'No Email and SMS data — Not exporting file';
END





end



