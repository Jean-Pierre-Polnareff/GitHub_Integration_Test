
CREATE VIEW [dbo].[VW_EVNTSAPI]
AS
	SELECT e.* 
	FROM dbo.[VW_CALLEXPORT] CM WITH (NOLOCK) 
		JOIN [dbo].CM_EVNTSAPI E WITH (NOLOCK) ON E.CALLID = CM.CALLID
GO
GRANT SELECT
    ON OBJECT::[dbo].[VW_EVNTSAPI] TO [CORP\pjain]
    AS [dbo];


GO
GRANT SELECT
    ON OBJECT::[dbo].[VW_EVNTSAPI] TO [CORP\rsingh]
    AS [dbo];


GO
GRANT SELECT
    ON OBJECT::[dbo].[VW_EVNTSAPI] TO [CORP\musalunke]
    AS [dbo];


GO
GRANT SELECT
    ON OBJECT::[dbo].[VW_EVNTSAPI] TO [CORP\apoddar]
    AS [dbo];


GO
GRANT SELECT
    ON OBJECT::[dbo].[VW_EVNTSAPI] TO [CORP\tkumar]
    AS [dbo];


GO
GRANT SELECT
    ON OBJECT::[dbo].[VW_EVNTSAPI] TO [corp\ravijaykumar]
    AS [dbo];


GO
GRANT SELECT
    ON OBJECT::[dbo].[VW_EVNTSAPI] TO [corp\pdwivedi]
    AS [dbo];


GO
GRANT SELECT
    ON OBJECT::[dbo].[VW_EVNTSAPI] TO [CORP\mhuang]
    AS [dbo];


GO
GRANT SELECT
    ON OBJECT::[dbo].[VW_EVNTSAPI] TO [CORP\dmukherji]
    AS [dbo];


GO
GRANT SELECT
    ON OBJECT::[dbo].[VW_EVNTSAPI] TO [CORP\aughodake]
    AS [dbo];


GO
GRANT SELECT
    ON OBJECT::[dbo].[VW_EVNTSAPI] TO [CORP\aramugade]
    AS [dbo];

