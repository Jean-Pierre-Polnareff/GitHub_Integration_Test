CREATE TABLE [dbo].[Restricted_Objects] (
    [name]      VARCHAR (100) NULL,
    [type_desc] VARCHAR (25)  NULL
);




GO
GRANT UPDATE
    ON OBJECT::[dbo].[Restricted_Objects] TO [CORP\dmukherji]
    AS [dbo];


GO
GRANT SELECT
    ON OBJECT::[dbo].[Restricted_Objects] TO [CORP\dmukherji]
    AS [dbo];


GO
GRANT DELETE
    ON OBJECT::[dbo].[Restricted_Objects] TO [CORP\dmukherji]
    AS [dbo];

