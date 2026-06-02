
CREATE VIEW [dbo].[VW_NONRPCREPORT]  AS  
	SELECT CM.*    FROM CM_NONRPCREPORT CM WITH (NOLOCK)     
		--JOIN [dbo].[VW_CALLEXPORT] E WITH (NOLOCK) ON E.CALLID = CM.CALLID            
GO
GRANT SELECT
    ON OBJECT::[dbo].[VW_NONRPCREPORT] TO [CORP\rsingh]
    AS [dbo];


GO
GRANT SELECT
    ON OBJECT::[dbo].[VW_NONRPCREPORT] TO [CORP\musalunke]
    AS [dbo];


GO
GRANT SELECT
    ON OBJECT::[dbo].[VW_NONRPCREPORT] TO [CORP\apoddar]
    AS [dbo];


GO
GRANT SELECT
    ON OBJECT::[dbo].[VW_NONRPCREPORT] TO [CORP\pjain]
    AS [dbo];


GO
GRANT SELECT
    ON OBJECT::[dbo].[VW_NONRPCREPORT] TO [CORP\mhuang]
    AS [dbo];


GO
GRANT SELECT
    ON OBJECT::[dbo].[VW_NONRPCREPORT] TO [CORP\tkumar]
    AS [dbo];


GO
GRANT SELECT
    ON OBJECT::[dbo].[VW_NONRPCREPORT] TO [corp\ravijaykumar]
    AS [dbo];


GO
GRANT SELECT
    ON OBJECT::[dbo].[VW_NONRPCREPORT] TO [corp\pdwivedi]
    AS [dbo];


GO
GRANT SELECT
    ON OBJECT::[dbo].[VW_NONRPCREPORT] TO [CORP\dmukherji]
    AS [dbo];


GO
GRANT SELECT
    ON OBJECT::[dbo].[VW_NONRPCREPORT] TO [CORP\aughodake]
    AS [dbo];


GO
GRANT SELECT
    ON OBJECT::[dbo].[VW_NONRPCREPORT] TO [CORP\aramugade]
    AS [dbo];

