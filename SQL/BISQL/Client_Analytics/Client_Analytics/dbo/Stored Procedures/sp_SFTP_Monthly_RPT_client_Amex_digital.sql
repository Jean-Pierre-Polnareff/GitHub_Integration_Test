USE [CLIENT_ANALYTICS]
GO

/****** Object:  StoredProcedure [dbo].[sp_SFTP_Monthly_RPT_client_Amex_digital]    Script Date: 5/4/2023 8:00:24 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO



ALTER PROCEDURE  [dbo].[sp_SFTP_Monthly_RPT_client_Amex_digital]
AS
BEGIN TRY
DECLARE @ErrorMessage VARCHAR(MAX) ;
DECLARE @subject1 VARCHAR(MAX);
DECLARE @body1 VARCHAR(MAX);
DECLARE @mmyy VARCHAR(MAX);
DECLARE @attachment VARCHAR(MAX);

SET NOCOUNT ON;

--Extract data into CSV file
DECLARE 
@vFeed       VARCHAR(1000) =  '\\dfw2-bisql-001\SSISFlatFileStage\ClientExport\AmexDigitalReport\Queue\Amex_Digital_Report_'  +  FORMAT(DATEADD(month,  DATEDIFF(month, 0, GETDATE() ) , -31),'MMM_yyyy') + '.csv',
@vZipped      VARCHAR(1000) =  'ccsdataAMEXDIGITALREPORT' + UPPER(FORMAT(DATEADD(month,  DATEDIFF(month, 0, GETDATE() ) , -31),'MMMMyyyy')) + '.zip',
@vBCP        VARCHAR(8000) = 'SET NOCOUNT ON; SELECT * FROM CLIENT_ANALYTICS.dbo.vw_Amex_Digital_Report ORDER BY 1 DESC'

SET @vBCP = 'bcp "' + @vBCP + '" queryout "' + @vFeed + '" -c -t, -T';
EXEC xp_cmdshell @vBCP, no_output;

-- Compress the CSV file
SET @vBCP = 'call C:\"Program Files"\7-Zip\7z a -tzip \\dfw2-bisql-001\SSISFlatFileStage\ClientExport\AmexDigitalReport\Zipped\' + @vZipped + ' ' + @vFeed;
EXEC xp_cmdshell @vBCP, no_output;

--FTP the zipped file
SET @vBCP = '\\dfw2-bisql-001\SSISFlatFileStage\ClientExport\AmexDigitalReport\WinSCP\WinSCP.com /ini=nul /command "open sftp://CCSINDIAPRD:Rt%ZgR#9Vf@fsgateway.aexp.com/ -hostkey=""ssh-rsa 2048 vaOdr81MW8EHwcqpsqn5BfiBabD5jHE3iwq9EW6ycD4=""" "lcd \\dfw2-bisql-001\SSISFlatFileStage\ClientExport\AmexDigitalReport\Zipped" "cd /inbox" "put -resumesupport=off ' + @vZipped + '" "exit"';
EXEC xp_cmdshell @vBCP, no_output;

-- Move file into Archive folder
SET @vBCP = 'move /Y \\dfw2-bisql-001\SSISFlatFileStage\ClientExport\AmexDigitalReport\Zipped\' + @vZipped + ' \\dfw2-bisql-001\SSISFlatFileStage\ClientExport\AmexDigitalReport\Archive\';
EXEC xp_cmdshell @vBCP, no_output;

----Notify end users 
SET @mmyy =    FORMAT(DATEADD(month,  DATEDIFF(month, 0, GETDATE() ) , -31),'MMMM yyyy') ;
              
SET @subject1 =  'Amex Digital Report for ' + @mmyy;


SET @body1 = 'Hi, 

The Amex Digital Report for ' + @mmyy + ' has been uploaded on SFTP server in the zip folder named ' + '"' +@vZipped + '"' + '.' + 
'

Kindly reach out to analytics@radiusgs.com if any issues or questions.

Thank you';

EXEC msdb.dbo.sp_send_dbmail
    @profile_name = 'DW Mail',
	@from_address ='dw@radiusgs.com',
    @recipients = 'Tushar.P.Chauhan@aexp.com; Nikhil.Taneja@aexp.com; Satyam.D.Singh@aexp.com; Aditya.V.Kumar@aexp.com; jayant.gautam@aexp.com', 
	@copy_recipients = 'Charles.Swamy@radiusgs.com; 
Dominic.Mileto@radiusgs.com;
Jithu.Mullur@radiusgs.com;
bilal.shaikh@radiusgs.com;
Bilal.Shaikh@aexp.com;
Jithu.Mullur1@aexp.com;
ted.miller@radiusgs.com;
doug.whitcomb@radiusgs.com;
bob.ruff@radiusgs.com;
Darpan.Thakkar@radiusgs.com;
Pulkit.Jain@radiusgs.com;
Analytics@radiusgs.com',
	@subject = @subject1,
    @body = @body1;
	
END TRY
BEGIN CATCH
    SELECT @ErrorMessage = @@SERVERNAME + '.' + DB_NAME() + '..' + OBJECT_NAME(@@PROCID) + ': ' + ERROR_MESSAGE();
	RAISERROR(@ErrorMessage,16,1);
END CATCH;
	
GO


