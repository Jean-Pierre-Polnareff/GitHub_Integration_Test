CREATE TABLE [dbo].[Restricted_Acct] (
    [acct_name] VARCHAR (50) NULL
);




GO
GRANT UPDATE
    ON OBJECT::[dbo].[Restricted_Acct] TO [CORP\dmukherji]
    AS [dbo];


GO
GRANT SELECT
    ON OBJECT::[dbo].[Restricted_Acct] TO [CORP\dmukherji]
    AS [dbo];


GO
GRANT DELETE
    ON OBJECT::[dbo].[Restricted_Acct] TO [CORP\dmukherji]
    AS [dbo];

