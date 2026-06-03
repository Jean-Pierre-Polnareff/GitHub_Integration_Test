

-- ================================= ============
-- Author:		Vladislav Pilipets
-- Create date: 2022-05-09
-- Description:	Unique CallID extraction for executing Events API
-- =============================================
CREATE PROCEDURE [dbo].[usp_07_SQL_Transcripts_Unique_ID]
	@mail_profile		varchar(50), 
	@mail_to			varchar(1500), 
	@mail_cc			varchar(1500) 
AS
BEGIN

SET NOCOUNT ON;

DECLARE @prefix_subject AS VARCHAR(50) = (SELECT attributes FROM Speech_Analytics.dbo.cm_connattr WHERE parameter = 'prefix' AND isActive = 1) 

SET QUOTED_IDENTIFIER ON

--Extracting Previous month from Last updated date from transcript table
IF OBJECT_ID('tempdb.dbo.#Results') IS NOT NULL 
DROP TABLE dbo.#Results 
SELECT EOMONTH(MAX(CALLDT),-1) AS ExeDate 
INTO dbo.#Results 
FROM dbo.CM_CALLEXPORT with (nolock) 

--Extracting Call details from Call export table with month end date extracted and with word count greater than 0
IF OBJECT_ID('tempdb.dbo.#Results1') IS NOT NULL 
DROP TABLE dbo.#Results1 
SELECT EOM,CALLDT,CALLID,ISNULL(SMPLFLG,0) AS SMPLFLG
INTO dbo.#Results1 
FROM dbo.CM_CALLEXPORT with (nolock) 
WHERE EOM >= (SELECT ExeDate FROM dbo.#Results) AND WRCNT>0 AND SMPLFLG IS NULL

--Extracting unique Calls processed in prevous executions from Transcript table with month end date extracted
IF OBJECT_ID('tempdb.dbo.#Results2') IS NOT NULL 
DROP TABLE dbo.#Results2
SELECT DISTINCT CALLID 
INTO dbo.#Results2
FROM dbo.CM_TRANSAPI with (nolock) 
WHERE EOM >= (SELECT ExeDate FROM dbo.#Results)

--Finding calls detla BETWEEN new calls AND processed calls
IF OBJECT_ID('tempdb.dbo.#Results3') IS NOT NULL 
DROP TABLE dbo.#Results3
SELECT A.EOM,A.CALLDT,A.CALLID,a.SMPLFLG,B.CALLID AS REMDT
INTO dbo.#Results3 
FROM dbo.#Results1 AS A 
LEFT JOIN dbo.#Results2 AS B ON A.CALLID=B.CALLID 
WHERE B.CALLID IS NULL

IF OBJECT_ID('tempdb.dbo.#Results3_1') IS NOT NULL 
DROP TABLE dbo.#Results3_1
SELECT B.EOM,B.CALLDT,B.CALLID,B.SMPLFLG 
INTO dbo.#Results3_1 
FROM (
SELECT EOM,CALLDT,CALLID,SMPLFLG,ROW_NUMBER() over (partition by CALLID order by CALLDT) rnk
FROM dbo.#Results3) B
WHERE B.rnk=1

--Updating Transcript pre stage table for API to exceute successfully
DELETE FROM dbo.CM_TRNSDT
WHERE EOM >= (SELECT ExeDate FROM dbo.#Results)
INSERT INTO dbo.CM_TRNSDT (EOM,CALLDT,CALLID,SMPLFLG)
SELECT DISTINCT EOM,CALLDT,CALLID,SMPLFLG 
FROM DBO.#Results3_1

DECLARE @totcnt VARCHAR (MAX);
SELECT @totcnt=CAST(COUNT(CALLID) AS varchar) FROM dbo.#Results3
PRINT @totcnt

DECLARE @body1 VARCHAR (MAX); 
		SET @body1 = 'Hi All,
		
Transcript API unique callid id process executed for '+convert(varchar,getdate(),102)+'.
		
Total count of call ids added for API execution are '+ (@totcnt) +'. 
		

Regards,
Business Analytics';
		print @body1
DECLARE @subject1 VARCHAR (MAX); 
		SET @subject1 = isnull(@prefix_subject,'') + 'Transcript API process executed for ' + convert(varchar,getdate(),102);
		print @subject1
	--send email
	if (SELECT count(CALLID) FROM dbo.#Results3)>0
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
    ON OBJECT::[dbo].[usp_07_SQL_Transcripts_Unique_ID] TO [CORP\aramugade]
    AS [dbo];


GO
GRANT VIEW DEFINITION
    ON OBJECT::[dbo].[usp_07_SQL_Transcripts_Unique_ID] TO [CORP\tkumar]
    AS [dbo];


GO
GRANT VIEW DEFINITION
    ON OBJECT::[dbo].[usp_07_SQL_Transcripts_Unique_ID] TO [CORP\aughodake]
    AS [dbo];


GO
GRANT VIEW DEFINITION
    ON OBJECT::[dbo].[usp_07_SQL_Transcripts_Unique_ID] TO [CORP\musalunke]
    AS [dbo];


GO
GRANT VIEW DEFINITION
    ON OBJECT::[dbo].[usp_07_SQL_Transcripts_Unique_ID] TO [CORP\pjain]
    AS [dbo];


GO
GRANT VIEW DEFINITION
    ON OBJECT::[dbo].[usp_07_SQL_Transcripts_Unique_ID] TO [CORP\mhuang]
    AS [dbo];

