

-- =============================================
-- Author:		Vladislav Pilipets
-- Create date: 2022-05-09
-- Description:	Unique CallID extraction for executing Events API
-- =============================================
CREATE PROCEDURE [dbo].[usp_03_SQL_Events_Unique_Compliance_ID]
	@mail_profile		varchar(50), 
	@mail_to			varchar(1500), 
	@mail_cc			varchar(1500) 
AS
BEGIN

SET NOCOUNT ON;

DECLARE @prefix_subject AS VARCHAR(50) = (SELECT attributes FROM Speech_Analytics.dbo.cm_connattr WHERE parameter = 'prefix' AND isActive = 1) 

--Extracting Previous month from Last updated date from events table
--Setting a eomdate for automatic run
DECLARE @Exe_Date DATETIME
SELECT @Exe_Date=dateadd(dd,-1,MAX(CALLDT))
FROM CM_CALLEXPORT with (nolock)  
PRINT @Exe_Date 

--Setting a eomdate for auto m atic run
DECLARE @End_Month DATETIME
SET @End_Month=EOMONTH(CAST(DATEADD(DAY,0,@Exe_Date) AS DATE ),-1) 
PRINT @End_Month 
---and RECDP='Veldos'

--Extracting Call details from Call export table with month end date extracted
IF OBJECT_ID('tempdb.dbo.#CALLS_SAMPLED') IS NOT NULL 
DROP TABLE dbo.#CALLS_SAMPLED  
SELECT EOM,CALLDT,CALLID,SMPLFLG
INTO dbo.#CALLS_SAMPLED  
FROM dbo.CM_CALLEXPORT with (nolock)  
WHERE EOM >= @End_Month 
	AND SMPLFLG=1

--Extracting unique Calls processed in prevous executions from events table with month end date extracted
IF OBJECT_ID('tempdb.dbo.#EVENTS_PROCESSED_MONTH') IS NOT NULL 
DROP TABLE dbo.#EVENTS_PROCESSED_MONTH  
SELECT DISTINCT CALLID 
INTO dbo.#EVENTS_PROCESSED_MONTH 
FROM dbo.CM_EVNTSAPICMP with (nolock)  
WHERE EOM>=@End_Month

--Finding calls detla BETWEEN new calls AND processed calls
IF OBJECT_ID('tempdb.dbo.#CALLS_SAMPLED_NOT_PROCESSED') IS NOT NULL 
DROP TABLE dbo.#CALLS_SAMPLED_NOT_PROCESSED
SELECT DISTINCT A.EOM,A.CALLDT,A.CALLID,A.SMPLFLG,B.CALLID AS REMDT
INTO dbo.#CALLS_SAMPLED_NOT_PROCESSED 
FROM dbo.#CALLS_SAMPLED AS A 
	LEFT JOIN dbo.#EVENTS_PROCESSED_MONTH AS B ON A.CALLID=B.CALLID 
WHERE B.CALLID IS NULL
 
--Updating Events pre stage table for API to exceute successfully
DELETE FROM dbo.CM_EVNTSDTCMP
WHERE EOM >= @End_Month 
INSERT INTO dbo.CM_EVNTSDTCMP (EOM,CALLDT,CALLID,SMPLFLG)
SELECT DISTINCT EOM,CALLDT,CALLID,SMPLFLG FROM DBO.#CALLS_SAMPLED_NOT_PROCESSED

DECLARE @totcnt VARCHAR (MAX);
SELECT @totcnt=CAST(COUNT(CALLID) AS VARCHAR) FROM dbo.#CALLS_SAMPLED_NOT_PROCESSED
PRINT @totcnt      


DECLARE @body1 VARCHAR (MAX); 
		SET @body1 = 'Hi All,
		
Events API unique callid id only for compliance items process executed for '+convert(VARCHAR,getdate(),102)+'.
		
Total count of call ids added for API execution are '+ (@totcnt) +'. 
		

Regards,
Business Analytics';
		print @body1
DECLARE @subject1 VARCHAR (MAX); 
		SET @subject1 = isnull(@prefix_subject,'') + 'Events API (Compliance) process executed for ' + convert(varchar,getdate(),102);
		print @subject1
	--send email
	if (SELECT count(CALLID) FROM dbo.#CALLS_SAMPLED_NOT_PROCESSED)>0
		EXEC msdb.dbo.sp_send_dbmail
		@profile_name = @mail_profile,
		@from_address ='Reports SpeechAnalytics <reports.speechanalytics@radiusgs.com>',
		@recipients='dw@radiusgs.com;business.analytics@radiusgs.com',

		@copy_recipients=@mail_cc,

		@subject = @subject1,

		@body = @body1;

END
GO
GRANT VIEW DEFINITION
    ON OBJECT::[dbo].[usp_03_SQL_Events_Unique_Compliance_ID] TO [CORP\aramugade]
    AS [dbo];


GO
GRANT VIEW DEFINITION
    ON OBJECT::[dbo].[usp_03_SQL_Events_Unique_Compliance_ID] TO [CORP\tkumar]
    AS [dbo];


GO
GRANT VIEW DEFINITION
    ON OBJECT::[dbo].[usp_03_SQL_Events_Unique_Compliance_ID] TO [CORP\aughodake]
    AS [dbo];


GO
GRANT VIEW DEFINITION
    ON OBJECT::[dbo].[usp_03_SQL_Events_Unique_Compliance_ID] TO [CORP\musalunke]
    AS [dbo];


GO
GRANT VIEW DEFINITION
    ON OBJECT::[dbo].[usp_03_SQL_Events_Unique_Compliance_ID] TO [CORP\pjain]
    AS [dbo];


GO
GRANT VIEW DEFINITION
    ON OBJECT::[dbo].[usp_03_SQL_Events_Unique_Compliance_ID] TO [CORP\mhuang]
    AS [dbo];

