
CREATE VIEW [dbo].[VW_TRNSDT]  AS   
	SELECT CM.*    
	FROM CM_TRNSDT CM WITH (NOLOCK)     
		JOIN [dbo].[VW_CALLEXPORT] E WITH (NOLOCK) ON E.CALLID = CM.CALLID
GO
GRANT SELECT
    ON OBJECT::[dbo].[VW_TRNSDT] TO [CORP\pjain]
    AS [dbo];


GO
GRANT SELECT
    ON OBJECT::[dbo].[VW_TRNSDT] TO [CORP\mhuang]
    AS [dbo];


GO
GRANT SELECT
    ON OBJECT::[dbo].[VW_TRNSDT] TO [CORP\tkumar]
    AS [dbo];


GO
GRANT SELECT
    ON OBJECT::[dbo].[VW_TRNSDT] TO [CORP\rsingh]
    AS [dbo];


GO
GRANT SELECT
    ON OBJECT::[dbo].[VW_TRNSDT] TO [corp\ravijaykumar]
    AS [dbo];


GO
GRANT SELECT
    ON OBJECT::[dbo].[VW_TRNSDT] TO [corp\pdwivedi]
    AS [dbo];


GO
GRANT SELECT
    ON OBJECT::[dbo].[VW_TRNSDT] TO [CORP\musalunke]
    AS [dbo];


GO
GRANT SELECT
    ON OBJECT::[dbo].[VW_TRNSDT] TO [CORP\dmukherji]
    AS [dbo];


GO
GRANT SELECT
    ON OBJECT::[dbo].[VW_TRNSDT] TO [CORP\aughodake]
    AS [dbo];


GO
GRANT SELECT
    ON OBJECT::[dbo].[VW_TRNSDT] TO [CORP\aramugade]
    AS [dbo];


GO
GRANT SELECT
    ON OBJECT::[dbo].[VW_TRNSDT] TO [CORP\apoddar]
    AS [dbo];

