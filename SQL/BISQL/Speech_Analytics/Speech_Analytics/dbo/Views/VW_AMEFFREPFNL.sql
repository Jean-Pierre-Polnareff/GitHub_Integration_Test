
CREATE VIEW [dbo].[VW_AMEFFREPFNL]
AS
	SELECT CM.* 
	FROM CM_AMEFFREPFNL CM WITH (NOLOCK) 
		LEFT JOIN [dbo].[CM_WHTLSTCLINTID] W WITH (NOLOCK) ON W.CLINTD = CM.CLNT_ID
									AND W.RECDP = CM.RECDP
	WHERE CM.RECDP = 'Veldos' 
		OR W.CLINTD IS NOT NULL
GO
GRANT SELECT
    ON OBJECT::[dbo].[VW_AMEFFREPFNL] TO [CORP\pjain]
    AS [dbo];


GO
GRANT SELECT
    ON OBJECT::[dbo].[VW_AMEFFREPFNL] TO [CORP\rsingh]
    AS [dbo];


GO
GRANT SELECT
    ON OBJECT::[dbo].[VW_AMEFFREPFNL] TO [CORP\musalunke]
    AS [dbo];


GO
GRANT SELECT
    ON OBJECT::[dbo].[VW_AMEFFREPFNL] TO [CORP\apoddar]
    AS [dbo];


GO
GRANT SELECT
    ON OBJECT::[dbo].[VW_AMEFFREPFNL] TO [CORP\tkumar]
    AS [dbo];


GO
GRANT SELECT
    ON OBJECT::[dbo].[VW_AMEFFREPFNL] TO [corp\ravijaykumar]
    AS [dbo];


GO
GRANT SELECT
    ON OBJECT::[dbo].[VW_AMEFFREPFNL] TO [corp\pdwivedi]
    AS [dbo];


GO
GRANT SELECT
    ON OBJECT::[dbo].[VW_AMEFFREPFNL] TO [CORP\mhuang]
    AS [dbo];


GO
GRANT SELECT
    ON OBJECT::[dbo].[VW_AMEFFREPFNL] TO [CORP\dmukherji]
    AS [dbo];


GO
GRANT SELECT
    ON OBJECT::[dbo].[VW_AMEFFREPFNL] TO [CORP\aughodake]
    AS [dbo];


GO
GRANT SELECT
    ON OBJECT::[dbo].[VW_AMEFFREPFNL] TO [CORP\aramugade]
    AS [dbo];

