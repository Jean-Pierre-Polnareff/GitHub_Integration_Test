USE [CLIENT_ANALYTICS]
GO
/****** Object:  StoredProcedure [dbo].[usp_client_amex_segment_missing_notify]    Script Date: 1/31/2022 3:12:14 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO




alter PROCEDURE [dbo].[usp_client_amex_segment_missing_notify]

AS
/* 
Object: usp_client_amex_segment_missing_notify

Description: Alert team on amex clientids with placements in last year
             needing a segment group added to CLIENT_ANALYTICS.dbo.Amex_Segment_Group

Author			Date		Description
Mike Campbell	05/03/2021	Created
*/

BEGIN
	SET NOCOUNT ON;

	DECLARE @body1 VARCHAR (MAX); 
		SET @body1 = '
		Amex clientids without Segment Group defined will not have Segment Group in Power BI reports.
		See attached.

		Please send Segment Group for attached to analytics@radiusgs.com.';


	--Send email on invalid Amex clientids
    --populate tmp tbl
	drop table if exists dw_staging.dbo.TMP_axp_clientid_missing_seggroup;

	SELECT dcu.ClientId
		   , v.Amex_Segment_Description
		   , v.Amex_Segment_Group
		   , COUNT(*) AS placements_in_last_year
    into dw_staging.dbo.TMP_axp_clientid_missing_seggroup
	FROM vw_Amex_ClientCodes_LookupTable v
		   RIGHT JOIN
		 DW_MSTR_DM.dbo.DimCustomer dcu ON v.ClientCode=dcu.ClientId
	WHERE dcu.ListDate>=DATEADD(YEAR,-1,GETDATE())
		  AND dcu.SourceSystem LIKE 'Amex%'
		  AND v.Amex_Segment_Group IS NULL
		  AND LEN(dcu.ClientId)>0
	GROUP BY dcu.ClientId
		   , v.Amex_Segment_Description
		   , v.Amex_Segment_Group;

	--send email
	if (select count(*) from dw_staging.dbo.TMP_axp_clientid_missing_seggroup)>0
		EXEC msdb.dbo.sp_send_dbmail
		@profile_name = @@SERVERNAME,--'DFW2-BISQL-001',
		@from_address ='dw@radiusgs.com',
		@recipients = 
		'bilal.shaikh@radiusgs.com;
		susheel.bhat@radiusgs.com;
		lawrence.budutie@radiusgs.com;
		annsneha.gonsalves@radiusgs.com;
		patrick.deprospo@radiusgs.com',

		@copy_recipients=
		'dw@radiusgs.com;
		bob.ruff@radiusgs.com',

		@subject = 'Amex clientids with missing Segment Group',

		@body = @body1,

		@query = 'select *
		from dw_staging.dbo.TMP_axp_clientid_missing_seggroup',

		@query_result_header=1, @attach_query_result_as_file=1;

END;
