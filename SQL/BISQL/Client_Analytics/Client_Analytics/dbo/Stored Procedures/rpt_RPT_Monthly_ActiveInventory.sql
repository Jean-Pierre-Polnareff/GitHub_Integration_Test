
CREATE PROCEDURE [dbo].[rpt_RPT_Monthly_ActiveInventory] AS 

TRUNCATE TABLE CLIENT_ANALYTICS.dbo.RPT_Monthly_ActiveInventoryTotal
TRUNCATE TABLE CLIENT_ANALYTICS.dbo.RPT_Monthly_ActiveInventoryHist

INSERT INTO dbo.RPT_Monthly_ActiveInventoryHist 
SELECT * FROM dbo.vw_FACS_RptActiveInventoryHist


INSERT INTO dbo.RPT_Monthly_ActiveInventoryTotal
SELECT client_stream,CLIENT_ID,
CUSTOMER_STATE,
worked_acct_ind,
PlacedMonth,
placed_amt,
placed_Accts FROM DW_MSTR_DM.dbo.TblRptActiveInventoryHist
GO
GRANT VIEW DEFINITION
    ON OBJECT::[dbo].[rpt_RPT_Monthly_ActiveInventory] TO [CORP\aramugade]
    AS [dbo];




GO
GRANT EXECUTE
    ON OBJECT::[dbo].[rpt_RPT_Monthly_ActiveInventory] TO [CORP\aramugade]
    AS [dbo];




GO
GRANT ALTER
    ON OBJECT::[dbo].[rpt_RPT_Monthly_ActiveInventory] TO [CORP\aramugade]
    AS [dbo];




GO



GO
GRANT VIEW DEFINITION
    ON OBJECT::[dbo].[rpt_RPT_Monthly_ActiveInventory] TO [corp\ravijaykumar]
    AS [dbo];


GO



GO
GRANT VIEW DEFINITION
    ON OBJECT::[dbo].[rpt_RPT_Monthly_ActiveInventory] TO [CORP\mhuang]
    AS [dbo];


GO



GO



GO
GRANT EXECUTE
    ON OBJECT::[dbo].[rpt_RPT_Monthly_ActiveInventory] TO [corp\ravijaykumar]
    AS [dbo];


GO



GO
GRANT EXECUTE
    ON OBJECT::[dbo].[rpt_RPT_Monthly_ActiveInventory] TO [CORP\mhuang]
    AS [dbo];


GO



GO



GO
GRANT ALTER
    ON OBJECT::[dbo].[rpt_RPT_Monthly_ActiveInventory] TO [corp\ravijaykumar]
    AS [dbo];


GO



GO
GRANT ALTER
    ON OBJECT::[dbo].[rpt_RPT_Monthly_ActiveInventory] TO [CORP\mhuang]
    AS [dbo];


GO
GRANT VIEW DEFINITION
    ON OBJECT::[dbo].[rpt_RPT_Monthly_ActiveInventory] TO [CORP\aughodake]
    AS [dbo];


GO
GRANT EXECUTE
    ON OBJECT::[dbo].[rpt_RPT_Monthly_ActiveInventory] TO [CORP\aughodake]
    AS [dbo];


GO
GRANT ALTER
    ON OBJECT::[dbo].[rpt_RPT_Monthly_ActiveInventory] TO [CORP\aughodake]
    AS [dbo];

