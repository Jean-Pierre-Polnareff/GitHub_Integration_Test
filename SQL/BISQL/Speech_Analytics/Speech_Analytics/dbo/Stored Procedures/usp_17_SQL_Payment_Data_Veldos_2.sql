

-- ================================= ============
-- Author:		Vladislav Pilipets
-- Create date: 2022-05-09
-- Description:	Updates payment received data only for veldos along with dollar amount
-- =============================================
CREATE PROCEDURE [dbo].[usp_17_SQL_Payment_Data_Veldos]
	@mail_profile		varchar(50), 
	@mail_to			varchar(1500), 
	@mail_cc			varchar(1500)
AS  
BEGIN

SET NOCOUNT ON;

DECLARE @prefix_subject AS VARCHAR(50) = (SELECT attributes FROM Speech_Analytics.dbo.cm_connattr WHERE parameter = 'prefix' AND isActive = 1) 

--Same day Payment amount update

--Start date based of the last payment amount updated
DECLARE @Start_Date DATE
SELECT @Start_Date= MAX(calldt) FROM dbo.CM_CALLEXPORT WHERE PAYRCVD IS NOT NULL AND RECDP='Veldos'
PRINT @Start_Date

DECLARE @Stop_Date DATE
SELECT @Stop_Date= CAST(DATEADD(DAY,-2,MAX(CALLDT)) AS DATE ) FROM dbo.CM_CALLEXPORT WHERE RECDP='Veldos'
PRINT @Stop_Date

--Extarcting Same day payment posting based of start and stop date
IF OBJECT_ID('tempdb.dbo.#Results') IS NOT NULL 
DROP TABLE dbo.#Results
SELECT a.[number],CAST(a.[received] AS DATE) rcvd,ROUND(b.totalpaid,0) ttpd,CAST(b.datepaid AS DATE) dtpaid
INTO dbo.#Results
FROM [HVDB02.CORPGLBDOM.LOCAL].[Amex].[dbo].[master] a
INNER JOIN [HVDB02.CORPGLBDOM.LOCAL].[Amex].[dbo].[payhistory] b ON	b.number = a.number 
WHERE b.batchtype NOT IN ('PCR','PUR') AND 
CAST(b.datepaid AS DATE) BETWEEN @Start_Date AND @Stop_Date
ORDER BY CAST(b.datepaid AS DATE)


--adding multiple payments made on sngle account
IF OBJECT_ID('tempdb.dbo.#Results1') IS NOT NULL 
DROP TABLE dbo.#Results1
SELECT a.[number],a.dtpaid,SUM(a.ttpd)  ttpd
INTO dbo.#Results1
FROM dbo.#Results a
GROUP BY  a.[number],a.dtpaid

--Updating the payment data based of filenumber and date of call
UPDATE dbo.CM_CALLEXPORT 
SET PAYRCVD=CAST(a.ttpd AS FLOAT)
FROM dbo.#Results1 a
WHERE CM_CALLEXPORT.RGSACC=CAST(a.number AS NVARCHAR(100)) 
AND CM_CALLEXPORT.CALLDT=a.dtpaid AND CM_CALLEXPORT.RECDP='Veldos'

--Updating the null values as 0 for executing process daily
UPDATE dbo.CM_CALLEXPORT
SET PAYRCVD=0
WHERE CM_CALLEXPORT.PAYRCVD IS NULL AND CM_CALLEXPORT.CALLDT<=@Stop_Date AND RECDP='Veldos'


DECLARE @totcnt VARCHAR (MAX);
SELECT @totcnt=CAST(COUNT(dtpaid) AS varchar) FROM dbo.#Results1
PRINT @totcnt

DECLARE @body1 VARCHAR (MAX); 
		SET @body1 = 'Hi All,
		
Payment Recieved process on Veldos Portal executed for '+convert(varchar,getdate(),23)+'.
		
Total count of accounts with dollar collection updtaed are '+ (@totcnt) +'. 
		

Regards,
Business Analytics';
		print @body1
DECLARE @subject1 VARCHAR (MAX); 
		SET @subject1 = isnull(@prefix_subject,'') + 'Payment Recieved(Veldos) process executed for ' + convert(varchar,getdate(),23);
		print @subject1
	--send email
	if (SELECT count(dtpaid) FROM dbo.#Results1)>0
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
    ON OBJECT::[dbo].[usp_17_SQL_Payment_Data_Veldos] TO [CORP\aramugade]
    AS [dbo];


GO
GRANT VIEW DEFINITION
    ON OBJECT::[dbo].[usp_17_SQL_Payment_Data_Veldos] TO [CORP\tkumar]
    AS [dbo];


GO
GRANT VIEW DEFINITION
    ON OBJECT::[dbo].[usp_17_SQL_Payment_Data_Veldos] TO [CORP\aughodake]
    AS [dbo];


GO
GRANT VIEW DEFINITION
    ON OBJECT::[dbo].[usp_17_SQL_Payment_Data_Veldos] TO [CORP\musalunke]
    AS [dbo];


GO
GRANT VIEW DEFINITION
    ON OBJECT::[dbo].[usp_17_SQL_Payment_Data_Veldos] TO [CORP\pjain]
    AS [dbo];


GO
GRANT VIEW DEFINITION
    ON OBJECT::[dbo].[usp_17_SQL_Payment_Data_Veldos] TO [CORP\mhuang]
    AS [dbo];

