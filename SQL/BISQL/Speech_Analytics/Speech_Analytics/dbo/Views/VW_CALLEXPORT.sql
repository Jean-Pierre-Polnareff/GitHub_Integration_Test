
CREATE VIEW [dbo].[VW_CALLEXPORT]
AS
	SELECT CM.* 
	FROM CM_CALLEXPORT CM WITH (NOLOCK) 
		LEFT JOIN [dbo].[CM_WHTLSTCLINTID] W WITH (NOLOCK) ON W.CLINTD = CM.CLNTID
									AND W.RECDP = CM.RECDP 
	WHERE CM.RECDP in ('Veldos','RGS_FrontLine') 
		OR W.CLINTD IS NOT NULL
GO
GRANT SELECT
    ON OBJECT::[dbo].[VW_CALLEXPORT] TO [CORP\pjain]
    AS [dbo];


GO
GRANT SELECT
    ON OBJECT::[dbo].[VW_CALLEXPORT] TO [CORP\rsingh]
    AS [dbo];


GO
GRANT SELECT
    ON OBJECT::[dbo].[VW_CALLEXPORT] TO [CORP\musalunke]
    AS [dbo];


GO
GRANT SELECT
    ON OBJECT::[dbo].[VW_CALLEXPORT] TO [CORP\apoddar]
    AS [dbo];


GO
GRANT SELECT
    ON OBJECT::[dbo].[VW_CALLEXPORT] TO [CORP\tkumar]
    AS [dbo];


GO
GRANT SELECT
    ON OBJECT::[dbo].[VW_CALLEXPORT] TO [corp\ravijaykumar]
    AS [dbo];


GO
GRANT SELECT
    ON OBJECT::[dbo].[VW_CALLEXPORT] TO [corp\pdwivedi]
    AS [dbo];


GO
GRANT SELECT
    ON OBJECT::[dbo].[VW_CALLEXPORT] TO [CORP\mhuang]
    AS [dbo];


GO
GRANT SELECT
    ON OBJECT::[dbo].[VW_CALLEXPORT] TO [CORP\dmukherji]
    AS [dbo];


GO
GRANT SELECT
    ON OBJECT::[dbo].[VW_CALLEXPORT] TO [CORP\aughodake]
    AS [dbo];


GO
GRANT SELECT
    ON OBJECT::[dbo].[VW_CALLEXPORT] TO [CORP\aramugade]
    AS [dbo];

