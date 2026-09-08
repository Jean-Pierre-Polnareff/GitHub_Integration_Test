

CREATE PROCEDURE [dbo].[rpt_transfer_mnsql5]
-- =============================================
--	Object: dbo.rpt_transfer_mnsql5
--
--  Description: temporararly brings over summaries from mnsql5 for reporting

-- 	History
-- 	Author		Date		Description
-- 	------------------------------------------------------
-- 	mcampbell	1/9/20		Created 
-- =======================================================================
AS

SET NOCOUNT ON;

	--EmailRPT_Clicks
	truncate table client_analytics.dbo.EmailRPT_Clicks;
	insert into client_analytics.dbo.EmailRPT_Clicks
	SELECT * FROM [MNSQL5.NORTHLANDGROUP.COM].ANALYTICS.dbo.vw_EmailRPT_Clicks;

	--EmailRPT_monthly
	truncate table client_analytics.dbo.EmailRPT_MonthClientLetter;
	insert into client_analytics.dbo.EmailRPT_MonthClientLetter
	select * from [MNSQL5.NORTHLANDGROUP.COM].analytics.dbo.vw_EmailRPT_MonthClientLetter;

	--EmailRPT_daily
	truncate table client_analytics.dbo.EmailRPT_daily;
	insert into client_analytics.dbo.EmailRPT_daily
	select * from [MNSQL5.NORTHLANDGROUP.COM].analytics.dbo.vw_EmailRPT_daily;

	--client_USBank_firstRPC
	--truncate table CLIENT_ANALYTICS.dbo.client_USBank_firstRPC;
	--insert into CLIENT_ANALYTICS.dbo.client_USBank_firstRPC
	--select * from [10.120.0.34].analytics.dbo.vw_client_USBank_firstRPC;
GO
GRANT VIEW DEFINITION
    ON OBJECT::[dbo].[rpt_transfer_mnsql5] TO [CORP\aramugade]
    AS [dbo];




GO
GRANT EXECUTE
    ON OBJECT::[dbo].[rpt_transfer_mnsql5] TO [CORP\aramugade]
    AS [dbo];




GO
GRANT ALTER
    ON OBJECT::[dbo].[rpt_transfer_mnsql5] TO [CORP\aramugade]
    AS [dbo];




GO



GO
GRANT VIEW DEFINITION
    ON OBJECT::[dbo].[rpt_transfer_mnsql5] TO [corp\ravijaykumar]
    AS [dbo];


GO



GO
GRANT VIEW DEFINITION
    ON OBJECT::[dbo].[rpt_transfer_mnsql5] TO [CORP\mhuang]
    AS [dbo];


GO



GO



GO
GRANT EXECUTE
    ON OBJECT::[dbo].[rpt_transfer_mnsql5] TO [corp\ravijaykumar]
    AS [dbo];


GO



GO
GRANT EXECUTE
    ON OBJECT::[dbo].[rpt_transfer_mnsql5] TO [CORP\mhuang]
    AS [dbo];


GO



GO



GO
GRANT ALTER
    ON OBJECT::[dbo].[rpt_transfer_mnsql5] TO [corp\ravijaykumar]
    AS [dbo];


GO



GO
GRANT ALTER
    ON OBJECT::[dbo].[rpt_transfer_mnsql5] TO [CORP\mhuang]
    AS [dbo];


GO
GRANT VIEW DEFINITION
    ON OBJECT::[dbo].[rpt_transfer_mnsql5] TO [CORP\aughodake]
    AS [dbo];


GO
GRANT EXECUTE
    ON OBJECT::[dbo].[rpt_transfer_mnsql5] TO [CORP\aughodake]
    AS [dbo];


GO
GRANT ALTER
    ON OBJECT::[dbo].[rpt_transfer_mnsql5] TO [CORP\aughodake]
    AS [dbo];

