
CREATE VIEW [dbo].[VW_EVNTSAPI_LossEx]
AS
	SELECT e.* 
	FROM [dbo].[CM_EVNTSAPI_LossEx] E WITH (NOLOCK)
GO
GRANT SELECT
    ON OBJECT::[dbo].[VW_EVNTSAPI_LossEx] TO [CORP\dmukherji]
    AS [dbo];


GO
GRANT SELECT
    ON OBJECT::[dbo].[VW_EVNTSAPI_LossEx] TO [CORP\smamidi]
    AS [dbo];


GO
GRANT SELECT
    ON OBJECT::[dbo].[VW_EVNTSAPI_LossEx] TO [CORP\tkumar]
    AS [dbo];


GO
GRANT SELECT
    ON OBJECT::[dbo].[VW_EVNTSAPI_LossEx] TO [corp\pdwivedi]
    AS [dbo];


GO
GRANT SELECT
    ON OBJECT::[dbo].[VW_EVNTSAPI_LossEx] TO [corp\ravijaykumar]
    AS [dbo];


GO
GRANT SELECT
    ON OBJECT::[dbo].[VW_EVNTSAPI_LossEx] TO [CORP\rsingh]
    AS [dbo];


GO
GRANT SELECT
    ON OBJECT::[dbo].[VW_EVNTSAPI_LossEx] TO [CORP\musalunke]
    AS [dbo];


GO
GRANT SELECT
    ON OBJECT::[dbo].[VW_EVNTSAPI_LossEx] TO [CORP\apoddar]
    AS [dbo];


GO
GRANT SELECT
    ON OBJECT::[dbo].[VW_EVNTSAPI_LossEx] TO [CORP\pjain]
    AS [dbo];

