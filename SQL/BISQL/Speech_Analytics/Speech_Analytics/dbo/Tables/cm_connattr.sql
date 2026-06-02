CREATE TABLE [dbo].[cm_connattr] (
    [parameter]  VARCHAR (50)   NULL,
    [attributes] VARCHAR (2048) NULL,
    [isActive]   BIT            NULL,
    [namespace]  VARCHAR (50)   NULL
);






GO
GRANT UPDATE
    ON OBJECT::[dbo].[cm_connattr] TO [CORP\dmukherji]
    AS [dbo];


GO
GRANT SELECT
    ON OBJECT::[dbo].[cm_connattr] TO [CORP\dmukherji]
    AS [dbo];


GO
GRANT DELETE
    ON OBJECT::[dbo].[cm_connattr] TO [CORP\dmukherji]
    AS [dbo];

