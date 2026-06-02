
CREATE VIEW [dbo].[VW_TRANSAPI]
AS
	SELECT E.* 
	FROM dbo.[VW_CALLEXPORT] CM WITH (NOLOCK) 
		JOIN [dbo].CM_TRANSAPI E WITH (NOLOCK) ON E.CALLID = CM.CALLID
GO
GRANT SELECT
    ON OBJECT::[dbo].[VW_TRANSAPI] TO [CORP\pjain]
    AS [dbo];


GO
GRANT SELECT
    ON OBJECT::[dbo].[VW_TRANSAPI] TO [CORP\rsingh]
    AS [dbo];


GO
GRANT SELECT
    ON OBJECT::[dbo].[VW_TRANSAPI] TO [CORP\musalunke]
    AS [dbo];


GO
GRANT SELECT
    ON OBJECT::[dbo].[VW_TRANSAPI] TO [CORP\apoddar]
    AS [dbo];


GO
GRANT SELECT
    ON OBJECT::[dbo].[VW_TRANSAPI] TO [CORP\tkumar]
    AS [dbo];


GO
GRANT SELECT
    ON OBJECT::[dbo].[VW_TRANSAPI] TO [corp\ravijaykumar]
    AS [dbo];


GO
GRANT SELECT
    ON OBJECT::[dbo].[VW_TRANSAPI] TO [corp\pdwivedi]
    AS [dbo];


GO
GRANT SELECT
    ON OBJECT::[dbo].[VW_TRANSAPI] TO [CORP\mhuang]
    AS [dbo];


GO
GRANT SELECT
    ON OBJECT::[dbo].[VW_TRANSAPI] TO [CORP\dmukherji]
    AS [dbo];


GO
GRANT SELECT
    ON OBJECT::[dbo].[VW_TRANSAPI] TO [CORP\aughodake]
    AS [dbo];


GO
GRANT SELECT
    ON OBJECT::[dbo].[VW_TRANSAPI] TO [CORP\aramugade]
    AS [dbo];

