
CREATE VIEW [dbo].[VW_Rpt_Lang_Pref]
AS
	SELECT e.* 
	FROM [dbo].[CM_Rpt_Lang_Pref] E WITH (NOLOCK)
GO
GRANT SELECT
    ON OBJECT::[dbo].[VW_Rpt_Lang_Pref] TO [CORP\dmukherji]
    AS [dbo];


GO
GRANT SELECT
    ON OBJECT::[dbo].[VW_Rpt_Lang_Pref] TO [CORP\smamidi]
    AS [dbo];


GO
GRANT SELECT
    ON OBJECT::[dbo].[VW_Rpt_Lang_Pref] TO [CORP\tkumar]
    AS [dbo];


GO
GRANT SELECT
    ON OBJECT::[dbo].[VW_Rpt_Lang_Pref] TO [corp\pdwivedi]
    AS [dbo];


GO
GRANT SELECT
    ON OBJECT::[dbo].[VW_Rpt_Lang_Pref] TO [corp\ravijaykumar]
    AS [dbo];


GO
GRANT SELECT
    ON OBJECT::[dbo].[VW_Rpt_Lang_Pref] TO [CORP\rsingh]
    AS [dbo];


GO
GRANT SELECT
    ON OBJECT::[dbo].[VW_Rpt_Lang_Pref] TO [CORP\musalunke]
    AS [dbo];


GO
GRANT SELECT
    ON OBJECT::[dbo].[VW_Rpt_Lang_Pref] TO [CORP\apoddar]
    AS [dbo];


GO
GRANT SELECT
    ON OBJECT::[dbo].[VW_Rpt_Lang_Pref] TO [CORP\pjain]
    AS [dbo];

