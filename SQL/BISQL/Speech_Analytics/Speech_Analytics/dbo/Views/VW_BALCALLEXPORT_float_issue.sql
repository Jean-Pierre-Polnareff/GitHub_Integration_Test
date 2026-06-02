

CREATE VIEW [dbo].[VW_BALCALLEXPORT_float_issue]
AS
	SELECT fi.* 
	FROM [dbo].[CM_BALCALLEXPORT_float_issue] fi 
		LEFT JOIN [dbo].[CM_CALLEXPORT] e on e.[CALLID] = fi.CALLID 
	WHERE e.CALLID IS NULL
GO
GRANT SELECT
    ON OBJECT::[dbo].[VW_BALCALLEXPORT_float_issue] TO [CORP\musalunke]
    AS [dbo];


GO
GRANT SELECT
    ON OBJECT::[dbo].[VW_BALCALLEXPORT_float_issue] TO [CORP\pjain]
    AS [dbo];


GO
GRANT SELECT
    ON OBJECT::[dbo].[VW_BALCALLEXPORT_float_issue] TO [CORP\mhuang]
    AS [dbo];


GO
GRANT SELECT
    ON OBJECT::[dbo].[VW_BALCALLEXPORT_float_issue] TO [CORP\tkumar]
    AS [dbo];


GO
GRANT SELECT
    ON OBJECT::[dbo].[VW_BALCALLEXPORT_float_issue] TO [CORP\dmukherji]
    AS [dbo];


GO
GRANT SELECT
    ON OBJECT::[dbo].[VW_BALCALLEXPORT_float_issue] TO [CORP\aughodake]
    AS [dbo];


GO
GRANT SELECT
    ON OBJECT::[dbo].[VW_BALCALLEXPORT_float_issue] TO [CORP\aramugade]
    AS [dbo];

