

-- =============================================
-- Author:		Vladislav Pilipets
-- Create date: 2022-05-09
-- Description:	Unique CallID extraction for executing Events API
-- =============================================
CREATE PROCEDURE [dbo].[usp_04_SQL_Events_Unique_ID]
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
FROM CM_CALLEXPORT
PRINT @Exe_Date  

--Setting a eomdate for automatic run
DECLARE @End_Month DATETIME
SET @End_Month=EOMONTH(CAST(DATEADD(DAY,0,@Exe_Date) AS DATE ),-1)
PRINT @End_Month

--Extracting Call details from Call export table with month end date extracted
IF OBJECT_ID('tempdb.dbo.#Results1') IS NOT NULL 
DROP TABLE dbo.#Results1 
SELECT EOM,CALLDT,CALLID,SMPLFLG,CATCNT,RECDP,CLNTID
INTO dbo.#Results1 
FROM dbo.CM_CALLEXPORT with (nolock) 
WHERE EOM>=@End_Month AND isnull(SMPLFLG,0)=0

---and RECDP='Veldos'

--Extracting unique Calls processed in prevous executions from events table with month end date extracted
IF OBJECT_ID('tempdb.dbo.#Results2') IS NOT NULL 
DROP TABLE dbo.#Results2
SELECT CALLID,count(*) as dt 
INTO dbo.#Results2
FROM dbo.CM_EVNTSAPI with (nolock) 
WHERE EOM >= @End_Month
group by CALLID

--Finding calls detla BETWEEN new calls AND processed calls
IF OBJECT_ID('tempdb.dbo.#Results3') IS NOT NULL 
DROP TABLE dbo.#Results3
SELECT DISTINCT A.EOM,A.CALLDT,A.CALLID,A.SMPLFLG,A.CATCNT,B.DT,C.CLINTD,a.recdp,b.callid as remdt,a.CLNTID,
CASE WHEN B.CALLID IS NULL THEN 1 ELSE 0 END AS CHECK1
INTO dbo.#Results3 
FROM dbo.#Results1 AS A 
LEFT JOIN dbo.#Results2 AS B ON A.CALLID=B.CALLID
left join  dbo.CM_WHTLSTCLINTID c on a.RECDP=c.RECDP and a.CLNTID=c.CLINTD

IF OBJECT_ID('tempdb.dbo.#Results3_1') IS NOT NULL 
DROP TABLE dbo.#Results3_1
SELECT DISTINCT A.EOM,A.CALLDT,A.CALLID,A.SMPLFLG,A.CATCNT,
A.DT,A.CLINTD,A.RECDP,A.REMDT,A.CLNTID,
CASE WHEn A.CHECK1=0 AND lower(A.recdp)<>'veldos' and
 A.CLINTD=A.CLNTID AND A.DT<(A.CATCNT-2) then 1 else A.CHECK1 end as CHECK1
--,CASE WHEN B.CALLID IS NULL THEN 1 ELSE 0 END AS CHECK1
INTO dbo.#Results3_1 
FROM dbo.#Results3 AS A 

IF OBJECT_ID('tempdb.dbo.#Results3_2') IS NOT NULL 
DROP TABLE dbo.#Results3_2
SELECT DISTINCT A.EOM,A.CALLDT,A.CALLID,A.SMPLFLG,A.CATCNT,
A.DT,A.CLINTD,A.RECDP,A.REMDT,A.CLNTID,
CASE WHEn A.CHECK1=0 AND lower(A.recdp)='veldos' and (A.DT is null OR a.dt<(a.CATCNT-2))
 then 1 else A.CHECK1 end as CHECK1
--,CASE WHEN B.CALLID IS NULL THEN 1 ELSE 0 END AS CHECK1
INTO dbo.#Results3_2 
FROM dbo.#Results3_1 AS A 

IF OBJECT_ID('tempdb.dbo.#Results3_3') IS NOT NULL 
DROP TABLE dbo.#Results3_3
SELECT DISTINCT A.EOM,A.CALLDT,A.CALLID,A.SMPLFLG
INTO dbo.#Results3_3 
FROM dbo.#Results3_2 A
where A.CHECK1=1

DELETE FROM DBO.CM_EVNTSAPI
WHERE CALLID IN (SELECT CALLID FROM DBO.#RESULTS3_3)

--Updating Events pre stage table for API to exceute successfully
DELETE FROM dbo.CM_EVNTSDT
WHERE EOM >= @End_Month
INSERT INTO dbo.CM_EVNTSDT (EOM,CALLDT,CALLID,SMPLFLG)
SELECT DISTINCT EOM,CALLDT,CALLID,SMPLFLG FROM DBO.#Results3_3

DECLARE @totcnt VARCHAR (MAX);
SELECT @totcnt=CAST(COUNT(CALLID) AS varchar) FROM dbo.#Results3_3
PRINT @totcnt


DECLARE @body1 VARCHAR (MAX); 
		SET @body1 = 'Hi All,
		
Events API unique callid id process executed for '+convert(varchar,getdate(),102)+'.
		
Total count of call ids added for API execution are '+ (@totcnt) +'. 
		

Regards,
Business Analytics';
		print @body1
DECLARE @subject1 VARCHAR (MAX); 
		SET @subject1 = isnull(@prefix_subject,'') + 'Events API process executed for ' + convert(varchar,getdate(),102);
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
    ON OBJECT::[dbo].[usp_04_SQL_Events_Unique_ID] TO [CORP\aramugade]
    AS [dbo];


GO
GRANT VIEW DEFINITION
    ON OBJECT::[dbo].[usp_04_SQL_Events_Unique_ID] TO [CORP\tkumar]
    AS [dbo];


GO
GRANT VIEW DEFINITION
    ON OBJECT::[dbo].[usp_04_SQL_Events_Unique_ID] TO [CORP\aughodake]
    AS [dbo];


GO
GRANT VIEW DEFINITION
    ON OBJECT::[dbo].[usp_04_SQL_Events_Unique_ID] TO [CORP\musalunke]
    AS [dbo];


GO
GRANT VIEW DEFINITION
    ON OBJECT::[dbo].[usp_04_SQL_Events_Unique_ID] TO [CORP\pjain]
    AS [dbo];


GO
GRANT VIEW DEFINITION
    ON OBJECT::[dbo].[usp_04_SQL_Events_Unique_ID] TO [CORP\mhuang]
    AS [dbo];

