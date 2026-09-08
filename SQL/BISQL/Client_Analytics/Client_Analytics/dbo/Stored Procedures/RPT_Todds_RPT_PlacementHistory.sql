CREATE PROC RPT_Todds_RPT_PlacementHistory AS

truncate TABLE CLIENT_ANALYTICS.dbo.Todds_RPT_PlacementHistory

INSERT INTO CLIENT_ANALYTICS.dbo.Todds_RPT_PlacementHistory 
SELECT * FROM DW_MSTR_DM.dbo.vw_Todds_RPT_PlacementHistory
GO
GRANT VIEW DEFINITION
    ON OBJECT::[dbo].[RPT_Todds_RPT_PlacementHistory] TO [CORP\aramugade]
    AS [dbo];




GO
GRANT EXECUTE
    ON OBJECT::[dbo].[RPT_Todds_RPT_PlacementHistory] TO [CORP\aramugade]
    AS [dbo];




GO
GRANT ALTER
    ON OBJECT::[dbo].[RPT_Todds_RPT_PlacementHistory] TO [CORP\aramugade]
    AS [dbo];




GO



GO
GRANT VIEW DEFINITION
    ON OBJECT::[dbo].[RPT_Todds_RPT_PlacementHistory] TO [corp\ravijaykumar]
    AS [dbo];


GO



GO
GRANT VIEW DEFINITION
    ON OBJECT::[dbo].[RPT_Todds_RPT_PlacementHistory] TO [CORP\mhuang]
    AS [dbo];


GO



GO



GO
GRANT EXECUTE
    ON OBJECT::[dbo].[RPT_Todds_RPT_PlacementHistory] TO [corp\ravijaykumar]
    AS [dbo];


GO



GO
GRANT EXECUTE
    ON OBJECT::[dbo].[RPT_Todds_RPT_PlacementHistory] TO [CORP\mhuang]
    AS [dbo];


GO



GO



GO
GRANT ALTER
    ON OBJECT::[dbo].[RPT_Todds_RPT_PlacementHistory] TO [corp\ravijaykumar]
    AS [dbo];


GO



GO
GRANT ALTER
    ON OBJECT::[dbo].[RPT_Todds_RPT_PlacementHistory] TO [CORP\mhuang]
    AS [dbo];


GO
GRANT VIEW DEFINITION
    ON OBJECT::[dbo].[RPT_Todds_RPT_PlacementHistory] TO [CORP\aughodake]
    AS [dbo];


GO
GRANT EXECUTE
    ON OBJECT::[dbo].[RPT_Todds_RPT_PlacementHistory] TO [CORP\aughodake]
    AS [dbo];


GO
GRANT ALTER
    ON OBJECT::[dbo].[RPT_Todds_RPT_PlacementHistory] TO [CORP\aughodake]
    AS [dbo];

