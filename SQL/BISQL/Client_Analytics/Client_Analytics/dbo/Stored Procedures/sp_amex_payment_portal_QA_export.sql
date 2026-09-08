USE [CLIENT_ANALYTICS]
GO
/****** Object:  StoredProcedure [dbo].[sp_amex_payment_portal_QA_export]    Script Date: 6/10/2022 5:49:22 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

ALTER PROCEDURE [dbo].[sp_amex_payment_portal_QA_export]

AS
BEGIN TRY

DECLARE 
@ErrorMessage					VARCHAR(MAX),
@subject1						VARCHAR(MAX),
@body1							VARCHAR(MAX),
@mmdd							CHAR(50)	= RIGHT(CONVERT(VARCHAR,GETDATE()-1,111),5),
@query1							VARCHAR(MAX),
@attach_query_result_as_file1	INT,
@attachment						VARCHAR(MAX),
@week_day						VARCHAR(10)	= DATENAME(weekday,GETDATE()),
@start_date						DATE		= cast(GetDate()-1 as date);
	
begin      
     
IF @week_day = 'Monday' BEGIN
  SET @start_date = CAST(GetDate()-3 AS DATE)
  SET @mmdd = RIGHT(CONVERT(VARCHAR,GETDATE()-3,111),5)+' - '+RIGHT(CONVERT(VARCHAR,GETDATE()-1,111),5);
END;

end

 drop table if exists #TMP_query_result

 ----preparing the data to be sent via excel and store it in temp table

SELECT * INTO  #TMP_query_result  FROM
(

select xyz.[CapturedOn]  as 'Captured Date'
	,xyz.[CustomerId]	as 'Customer Id'
	,xyz.SourceSystem as 'CRM'	
	,xyz.[ClientId]	
	,xyz.[Key]	
	,isnull (xyz.[Description], '') as [Description]
	,isnull (case when xyz.[Key]= 'program_enrollment' then '' else a.[Key] end ,'') as Paymethod
	from DW_MSTR_DM.dbo.Web_Waterfall_Metrics_Data_Client (nolock) as xyz

	left join (
	select [Capturedon], [CustomerId], Sourcesystem, Clientid, [Key], KeyCustomer
	
	from DW_MSTR_DM.dbo.Web_Waterfall_Metrics_Data_Client (nolock)

	where SourceSystem = 'AMEX Latitude' 
    and [Key] IN ('debit_card_used','checking_account_used','credit_card_used')

	and CapturedOn >= @start_date
	) as a

	on xyz.CapturedOn  = a.CapturedOn 
	and xyz.SourceSystem=a.SourceSystem
	and xyz.ClientId=a.ClientId
	and xyz.CustomerID=a.CustomerID
	and xyz.KeyCustomer=a.keycustomer

	where xyz.SourceSystem = 'AMEX Latitude' 
    and xyz.[Key] IN ('installment_submitted','one_time_payment_submitted','pay_in_full_submitted','program_enrollment','settlement_submitted')
	and xyz.CapturedOn >= @start_date
    ---order by [Captured Date] desc

) AS A1;


----check if there is any data and attach file accordingly
IF (SELECT COUNT(*) AS No_of_Rows FROM #TMP_query_result ) > 0 
SET @attach_query_result_as_file1 = 1
ELSE
SET @attach_query_result_as_file1 = 0;
SET @attachment = 'AXP_Payment_Portal_QA_' + convert(varchar(8),cast(GETDATE() -1  as date),112) + '.csv';


IF DATENAME(weekday, GETDATE()) = 'Monday' 
SET @mmdd = RIGHT(CONVERT(VARCHAR ,GETDATE()-3, 111), 5) + ' - ' + RIGHT(CONVERT(VARCHAR ,GETDATE()-1, 111), 5);
else
SET @mmdd = RIGHT(CONVERT(VARCHAR ,GETDATE()-1, 111), 5);

----Subject for email with date-------
SET @subject1 = 'AXP Payment Portal QA for ' +  @mmdd ;

----Email body when there is data/ no data
IF (SELECT COUNT(*) AS No_of_Rows FROM #TMP_query_result ) > 0 and (select distinct count([Key]) from #TMP_query_result where [Key]='program_enrollment') > 0

SET @body1 = 'Hi All,

Please see attached the report for AXP Payment Portal QA for ' + @mmdd + '.

Reach out to analytics@radiusgs.com if you have any questions.

Thanks'

ELSE IF (SELECT COUNT(*) AS No_of_Rows FROM #TMP_query_result ) > 0 and (select distinct count([Key]) from #TMP_query_result where [Key]='program_enrollment') = 0

SET @body1 = 'Hi All,

Please see attached the report for AXP Payment Portal QA for ' + @mmdd + '.

Note: No data found of program_enrollment for ' + @mmdd + '.

Reach out to analytics@radiusgs.com if you have any questions.

Thanks'

ELSE 
SET @body1 = 'Hi All,

No data found for ' + @mmdd + '.
Reach out to analytics@radiusgs.com if you have any questions.

Thanks'
;

----Content in the .csv file

IF (SELECT COUNT(*) AS No_of_Rows FROM #TMP_query_result ) > 0 
  SET @query1 = 'SET NOCOUNT ON; 
	SELECT ''Captured Date'' [Captured Date], ''Customer Id'' [Customer Id], ''CRM'' [CRM], ''ClientId'' [ClientId], ''Key'' [Key], ''Description'' [Description], ''Paymethod'' [Paymethod]
	UNION
	select CAST(xyz.[CapturedOn] AS VARCHAR)  as ''Captured Date''
	,CAST(xyz.[CustomerId] AS VARCHAR)	as ''Customer Id''
	,xyz.SourceSystem as ''CRM''
	,xyz.[ClientId]	
	,xyz.[Key]	
	,isnull (xyz.[Description], '''') as [Description]
	,isnull (case when xyz.[Key]= ''program_enrollment'' then '''' else a.[Key] end ,'''') as Paymethod
	from DW_MSTR_DM.dbo.Web_Waterfall_Metrics_Data_Client (nolock) as xyz
	left join (
	select [Capturedon], [CustomerId], Sourcesystem, Clientid, [Key], Keycustomer
	from DW_MSTR_DM.dbo.Web_Waterfall_Metrics_Data_Client (nolock)
	where SourceSystem = ''AMEX Latitude''
    and [Key] IN (''debit_card_used'',''checking_account_used'',''credit_card_used'')
	and CapturedOn >= ''' + CAST(@start_date AS VARCHAR) + '''
	) as a
	on xyz.CapturedOn  = a.CapturedOn 
	and xyz.SourceSystem=a.SourceSystem
	and xyz.ClientId=a.ClientId
	and xyz.keyCustomer=a.keyCustomer
	where xyz.SourceSystem = ''AMEX Latitude'' 
    and xyz.[Key] IN (''installment_submitted'',''one_time_payment_submitted'',''pay_in_full_submitted'',''program_enrollment'',''settlement_submitted'')
	and xyz.CapturedOn >= ''' + CAST(@start_date AS VARCHAR) + '''
	union 
	select
    '' 
    Applied filters:
    CRM is AMEX Latitude
    Key is installment_submitted, one_time_payment_submitted, pay_in_full_submitted, settlement_submitted, program_enrollment, 
    Captured Date Range ' + @mmdd + ' '' [Captured Date], '''' [Customer Id], '''' [CRM], '''' [ClientId], '''' [Key], '''' [Description], '''' [Paymethod]
	ORDER BY 1 DESC' 

	else

    SET @query1 = 'SET NOCOUNT ON; 
	SELECT ''''';


-----to send an email----------

IF @week_day NOT IN ('Saturday','Sunday')

EXEC msdb.dbo.sp_send_dbmail
    @profile_name = 'DW Mail',
	@from_address ='dw@radiusgs.com',
    @recipients = 'bob.ruff@radiusgs.com; laura.dailey@radiusgs.com; amexpayments@radiusgs.com', 
	@copy_recipients = 'Mike.Campbell@radiusgs.com; ted.miller@radiusgs.com; gar.donecker@radiusgs.com; analytics@radiusgs.com; Jithu.Mullur@radiusgs.com; Tanmay.Goraksha@radiusgs.com;
	                    Samar.Reddy@radiusgs.com; Subhash.Gupta@radiusgs.com',
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
  
  else

  select * from #TMP_query_result
	
END TRY
BEGIN CATCH
SELECT @ErrorMessage = @@SERVERNAME + '.' + DB_NAME() + '..' + OBJECT_NAME(@@PROCID) + ': ' + ERROR_MESSAGE();
RAISERROR(@ErrorMessage,16,1);
END CATCH;
GO

