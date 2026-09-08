


CREATE PROCEDURE [dbo].[rpt_transfer_mnsql5_daily]
-- =============================================
--	Object: dbo.rpt_transfer_mnsql5_daily
--
--  Description: temporararly brings over summaries from mnsql5 for reporting

-- 	History
-- 	Author		Date		Description
-- 	------------------------------------------------------
-- 	mcampbell	2/7/20		Created 
-- =======================================================================
AS

SET NOCOUNT ON;

	----EmailRPT_Clicks
	--TRUNCATE TABLE client_analytics.dbo.EmailRPT_Clicks;
	--INSERT INTO client_analytics.dbo.EmailRPT_Clicks
	--SELECT * FROM [10.120.0.34].analytics.dbo.vw_EmailRPT_Clicks;

	----EmailRPT_monthly
	--TRUNCATE TABLE client_analytics.dbo.EmailRPT_MonthClientLetter;
	--INSERT INTO client_analytics.dbo.EmailRPT_MonthClientLetter
	--SELECT * FROM [10.120.0.34].analytics.dbo.vw_EmailRPT_MonthClientLetter;

	----EmailRPT_daily
	--TRUNCATE TABLE client_analytics.dbo.EmailRPT_daily;
	--INSERT INTO client_analytics.dbo.EmailRPT_daily
	--SELECT * FROM [10.120.0.34].analytics.dbo.vw_EmailRPT_daily;

	--client_USBank_firstRPC
	truncate table CLIENT_ANALYTICS.dbo.client_USBank_firstRPC;
	insert into CLIENT_ANALYTICS.dbo.client_USBank_firstRPC
	select * from [MNSQL5.NORTHLANDGROUP.COM].analytics.dbo.RPT_USBank_first_RPC;

	truncate table CLIENT_ANALYTICS.dbo.client_USBank_firstRPC_day;
	insert into CLIENT_ANALYTICS.dbo.client_USBank_firstRPC_day
	select * from [MNSQL5.NORTHLANDGROUP.COM].analytics.dbo.RPT_USBank_first_RPC_day;
GO
GRANT VIEW DEFINITION
    ON OBJECT::[dbo].[rpt_transfer_mnsql5_daily] TO [CORP\aramugade]
    AS [dbo];




GO
GRANT EXECUTE
    ON OBJECT::[dbo].[rpt_transfer_mnsql5_daily] TO [CORP\aramugade]
    AS [dbo];




GO
GRANT ALTER
    ON OBJECT::[dbo].[rpt_transfer_mnsql5_daily] TO [CORP\aramugade]
    AS [dbo];




GO



GO
GRANT VIEW DEFINITION
    ON OBJECT::[dbo].[rpt_transfer_mnsql5_daily] TO [corp\ravijaykumar]
    AS [dbo];


GO



GO
GRANT VIEW DEFINITION
    ON OBJECT::[dbo].[rpt_transfer_mnsql5_daily] TO [CORP\mhuang]
    AS [dbo];


GO



GO



GO
GRANT EXECUTE
    ON OBJECT::[dbo].[rpt_transfer_mnsql5_daily] TO [corp\ravijaykumar]
    AS [dbo];


GO



GO
GRANT EXECUTE
    ON OBJECT::[dbo].[rpt_transfer_mnsql5_daily] TO [CORP\mhuang]
    AS [dbo];


GO



GO



GO
GRANT ALTER
    ON OBJECT::[dbo].[rpt_transfer_mnsql5_daily] TO [corp\ravijaykumar]
    AS [dbo];


GO



GO
GRANT ALTER
    ON OBJECT::[dbo].[rpt_transfer_mnsql5_daily] TO [CORP\mhuang]
    AS [dbo];


GO
GRANT VIEW DEFINITION
    ON OBJECT::[dbo].[rpt_transfer_mnsql5_daily] TO [CORP\aughodake]
    AS [dbo];


GO
GRANT EXECUTE
    ON OBJECT::[dbo].[rpt_transfer_mnsql5_daily] TO [CORP\aughodake]
    AS [dbo];


GO
GRANT ALTER
    ON OBJECT::[dbo].[rpt_transfer_mnsql5_daily] TO [CORP\aughodake]
    AS [dbo];

