CREATE VIEW VW_SMS_Summary_Raw
AS
	SELECT e.* 
	FROM [dbo].CM_SMS_Summary_Raw E WITH (NOLOCK)
GO
GRANT SELECT
    ON OBJECT::[dbo].[VW_SMS_Summary_Raw] TO [CORP\dmukherji]
    AS [dbo];


GO
GRANT SELECT
    ON OBJECT::[dbo].[VW_SMS_Summary_Raw] TO [CORP\tkumar]
    AS [dbo];


GO
GRANT SELECT
    ON OBJECT::[dbo].[VW_SMS_Summary_Raw] TO [corp\pdwivedi]
    AS [dbo];


GO
GRANT SELECT
    ON OBJECT::[dbo].[VW_SMS_Summary_Raw] TO [corp\ravijaykumar]
    AS [dbo];


GO
GRANT SELECT
    ON OBJECT::[dbo].[VW_SMS_Summary_Raw] TO [CORP\rsingh]
    AS [dbo];


GO
GRANT SELECT
    ON OBJECT::[dbo].[VW_SMS_Summary_Raw] TO [CORP\musalunke]
    AS [dbo];


GO
GRANT SELECT
    ON OBJECT::[dbo].[VW_SMS_Summary_Raw] TO [CORP\apoddar]
    AS [dbo];


GO
GRANT SELECT
    ON OBJECT::[dbo].[VW_SMS_Summary_Raw] TO [CORP\pjain]
    AS [dbo];

