
CREATE VIEW [dbo].[VW_CXANALY]  AS  
	SELECT CM.*    FROM CM_CXANALY CM WITH (NOLOCK)     
		--JOIN [dbo].[VW_CALLEXPORT] E WITH (NOLOCK) ON E.CALLID = CM.CALLID            
GO
GRANT SELECT
    ON OBJECT::[dbo].[VW_CXANALY] TO [CORP\rsingh]
    AS [dbo];


GO
GRANT SELECT
    ON OBJECT::[dbo].[VW_CXANALY] TO [CORP\musalunke]
    AS [dbo];


GO
GRANT SELECT
    ON OBJECT::[dbo].[VW_CXANALY] TO [CORP\apoddar]
    AS [dbo];


GO
GRANT SELECT
    ON OBJECT::[dbo].[VW_CXANALY] TO [CORP\pjain]
    AS [dbo];


GO
GRANT SELECT
    ON OBJECT::[dbo].[VW_CXANALY] TO [CORP\mhuang]
    AS [dbo];


GO
GRANT SELECT
    ON OBJECT::[dbo].[VW_CXANALY] TO [CORP\tkumar]
    AS [dbo];


GO
GRANT SELECT
    ON OBJECT::[dbo].[VW_CXANALY] TO [corp\ravijaykumar]
    AS [dbo];


GO
GRANT SELECT
    ON OBJECT::[dbo].[VW_CXANALY] TO [corp\pdwivedi]
    AS [dbo];


GO
GRANT SELECT
    ON OBJECT::[dbo].[VW_CXANALY] TO [CORP\dmukherji]
    AS [dbo];


GO
GRANT SELECT
    ON OBJECT::[dbo].[VW_CXANALY] TO [CORP\aughodake]
    AS [dbo];


GO
GRANT SELECT
    ON OBJECT::[dbo].[VW_CXANALY] TO [CORP\aramugade]
    AS [dbo];

