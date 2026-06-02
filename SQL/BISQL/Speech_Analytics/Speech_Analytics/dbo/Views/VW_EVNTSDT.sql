
CREATE VIEW [dbo].[VW_EVNTSDT]  AS   
	SELECT CM.*    
	FROM CM_EVNTSDT CM WITH (NOLOCK)     
		JOIN [dbo].[VW_CALLEXPORT] E WITH (NOLOCK) ON E.CALLID = CM.CALLID
GO
GRANT SELECT
    ON OBJECT::[dbo].[VW_EVNTSDT] TO [CORP\pjain]
    AS [dbo];


GO
GRANT SELECT
    ON OBJECT::[dbo].[VW_EVNTSDT] TO [CORP\mhuang]
    AS [dbo];


GO
GRANT SELECT
    ON OBJECT::[dbo].[VW_EVNTSDT] TO [CORP\tkumar]
    AS [dbo];


GO
GRANT SELECT
    ON OBJECT::[dbo].[VW_EVNTSDT] TO [CORP\rsingh]
    AS [dbo];


GO
GRANT SELECT
    ON OBJECT::[dbo].[VW_EVNTSDT] TO [corp\ravijaykumar]
    AS [dbo];


GO
GRANT SELECT
    ON OBJECT::[dbo].[VW_EVNTSDT] TO [corp\pdwivedi]
    AS [dbo];


GO
GRANT SELECT
    ON OBJECT::[dbo].[VW_EVNTSDT] TO [CORP\musalunke]
    AS [dbo];


GO
GRANT SELECT
    ON OBJECT::[dbo].[VW_EVNTSDT] TO [CORP\dmukherji]
    AS [dbo];


GO
GRANT SELECT
    ON OBJECT::[dbo].[VW_EVNTSDT] TO [CORP\aughodake]
    AS [dbo];


GO
GRANT SELECT
    ON OBJECT::[dbo].[VW_EVNTSDT] TO [CORP\aramugade]
    AS [dbo];


GO
GRANT SELECT
    ON OBJECT::[dbo].[VW_EVNTSDT] TO [CORP\apoddar]
    AS [dbo];

