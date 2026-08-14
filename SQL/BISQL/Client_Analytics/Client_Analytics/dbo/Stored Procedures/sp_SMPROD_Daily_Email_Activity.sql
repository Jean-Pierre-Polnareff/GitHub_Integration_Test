

CREATE PROCEDURE [dbo].[sp_SMPROD_Daily_Email_Activity] 
AS
BEGIN
    
    DECLARE @startDate DATE = DATEADD(MONTH, DATEDIFF(MONTH, 0, GETDATE() - 1), 0);
    DECLARE @endDate DATE = DATEADD(DAY, -1, GETDATE());
    DECLARE @subject VARCHAR(MAX);
    DECLARE @body VARCHAR(MAX);
    DECLARE @tab CHAR(1) = CHAR(9);
    DECLARE @File_name VARCHAR(100) = 'SMPROD_Daily_Email_Activity_' + FORMAT(GETDATE() - 1, 'MMddyy') + '.csv';

    -- Email subject
    SET @subject = 'SMPROD Daily Email Activity for: ' + CONVERT(VARCHAR, GETDATE() - 1, 106);

   
    IF OBJECT_ID('tempdb..##EmailActivity') IS NOT NULL
        DROP TABLE ##EmailActivity;

    SELECT 
        CAST(send_date AS DATE) AS [Send Date]
       ,crm AS CRM 
       ,client AS Client
       ,clientid AS ClientID
       ,SUM(requested) AS Requested
       ,SUM([sent]) AS [Sent]
       ,SUM(delivered) AS Delivered
       ,SUM(unq_opens) AS [Unique Opened]
       ,SUM(total_opens) AS [Overall Opened]
       ,SUM(bounced) AS Bounced,
        SUM(clicks) AS Clicks
		INTO ##EmailActivity
    FROM [CLIENT_ANALYTICS].dbo.RPT_email_guid
    WHERE crm = 'SMPROD' AND clientid <> 'warmup test'
    AND send_date >= @startDate AND send_date <= @endDate
    GROUP BY CAST(send_date AS DATE), crm, client, clientid;
	
	
    -- CSS style block to get html style for mail purpose
    DECLARE @style NVARCHAR(MAX) = N'
    <style>
        table.cinereousTable {
            border: 1px solid #000000;
            background-color: #FFFAF8;
            text-align: center;
            border-collapse: collapse;
            width: 100%;
        }
        table.cinereousTable td, table.cinereousTable th {
            border: 1px solid #000000;
            padding: 4px 4px;
        }
        table.cinereousTable tbody td {
            font-size: 13px;
        }
        table.cinereousTable thead {
            background: #FFB30D;
            font-weight: bold;
            background: linear-gradient(to bottom, #ffc649 0%, #ffba25 66%, #FFB30D 100%);
        }
        table.cinereousTable thead th {
            font-size: 17px;
            font-weight: bold;
            color: #070707;
            text-align: center;
        }
        p {
            font-family: verdana;
            font-size: 12px;
        }
    </style>';
 
    -- Initialize HTML Code for Email table in body
    DECLARE @tableHTML NVARCHAR(MAX) = N'';
    SET @tableHTML = 
    N'<html><head>' + @style + N'</head><body>' +
    N'<p>Hi All,</p>' +
    N'<p>Please find attached the Daily SMPROD Email Activity Data.' +
    N'<p>Below is the table as a snapshot of the attached file.</p>' +
    N'<p><strong>'+'</strong></p>'+
    N'<table class="cinereousTable">' +
    N'<thead><tr>' +
    N'<th>Send date</th><th>CRM</th><th>Client</th><th>ClientID</th>' +
    N'<th>Requested</th><th>Sent</th><th>Delivered</th><th>Unique Opened</th>' +
    N'<th>Overall Opened</th><th>Bounced</th><th>Clicks</th>' +
    N'</tr></thead><tbody>';
 
    -- Appending data rows
    SELECT @tableHTML = @tableHTML +
        N'<tr>' +
        N'<td><b>' + ISNULL(CONVERT(VARCHAR, [Send Date], 23), '') + N'</b></td>' + 
        N'<td>' + ISNULL([CRM], '') + N'</td>' +  
        N'<td>' + ISNULL([Client], '') + N'</td>' +  
        N'<td>' + ISNULL([ClientID], '') + N'</td>' +  
        N'<td>' + CAST(ISNULL([Requested], 0) AS NVARCHAR(20)) + N'</td>' +  
        N'<td>' + CAST(ISNULL([Sent], 0) AS NVARCHAR(20)) + N'</td>' +  
        N'<td>' + CAST(ISNULL([Delivered], 0) AS NVARCHAR(20)) + N'</td>' + 
        N'<td>' + CAST(ISNULL([Unique Opened], 0) AS NVARCHAR(20)) + N'</td>' +  
        N'<td>' + CAST(ISNULL([Overall Opened], 0) AS NVARCHAR(20)) + N'</td>' +  
        N'<td>' + CAST(ISNULL([Bounced], 0) AS NVARCHAR(20)) + N'</td>' +  
        N'<td>' + CAST(ISNULL([Clicks], 0) AS NVARCHAR(20)) + N'</td>' +  
        N'</tr>'
    FROM ##EmailActivity;
 
    SET @tableHTML = @tableHTML + 

    N'</tbody></table>' +
        N'<p>Please let us know if any questions/issues.</p>' +
 
   
    N'<p>Regards,<br>Business Analytics Team</p></body></html>';
  

    -- Sending mail with CSV attachment using global temp table
    EXEC msdb.dbo.sp_send_dbmail
        @profile_name = 'DW Mail',
        @from_address = '_Group - Data Warehousing <dw@radiusgs.com>',
        @recipients = 'Sankeerth.Mamidi@radiusgs.com',
        @copy_recipients = 'Amod.Ramugade@radiusgs.com',
        @subject = @subject,
        @body = @tableHTML,
		@body_format='HTML',
        @query = 'SELECT * FROM ##EmailActivity',
        @execute_query_database = 'CLIENT_ANALYTICS',
        @query_result_header = 1,
        @attach_query_result_as_file = 1,
        @query_attachment_filename = @File_name,
        @query_result_separator = @tab,
        @query_result_no_padding = 1,
        @query_result_width = 32767;

END