CREATE TABLE [dbo].[CM_DIMAXPRcodes] (
    [CLINTID]      NVARCHAR (150) NOT NULL,
    [Ops_Category] NVARCHAR (150) NOT NULL,
    [SA_Category]  NVARCHAR (150) NOT NULL
);


GO
GRANT UPDATE
    ON OBJECT::[dbo].[CM_DIMAXPRcodes] TO [CORP\dmukherji]
    AS [dbo];


GO
GRANT SELECT
    ON OBJECT::[dbo].[CM_DIMAXPRcodes] TO [CORP\dmukherji]
    AS [dbo];


GO
GRANT DELETE
    ON OBJECT::[dbo].[CM_DIMAXPRcodes] TO [CORP\dmukherji]
    AS [dbo];


GO
GRANT UPDATE
    ON OBJECT::[dbo].[CM_DIMAXPRcodes] TO [CORP\pjain]
    AS [dbo];


GO
GRANT SELECT
    ON OBJECT::[dbo].[CM_DIMAXPRcodes] TO [CORP\pjain]
    AS [dbo];


GO
GRANT INSERT
    ON OBJECT::[dbo].[CM_DIMAXPRcodes] TO [CORP\pjain]
    AS [dbo];


GO
GRANT DELETE
    ON OBJECT::[dbo].[CM_DIMAXPRcodes] TO [CORP\pjain]
    AS [dbo];

