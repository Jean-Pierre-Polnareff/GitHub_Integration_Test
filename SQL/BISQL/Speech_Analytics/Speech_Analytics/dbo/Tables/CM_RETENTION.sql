CREATE TABLE [dbo].[CM_RETENTION] (
    [Table_Name] VARCHAR (50) NULL,
    [days]       SMALLINT     NULL,
    [IsActive]   BIT          DEFAULT ((1)) NULL,
    [sort]       SMALLINT     NULL
);




GO
GRANT UPDATE
    ON OBJECT::[dbo].[CM_RETENTION] TO [CORP\dmukherji]
    AS [dbo];


GO
GRANT SELECT
    ON OBJECT::[dbo].[CM_RETENTION] TO [CORP\dmukherji]
    AS [dbo];


GO
GRANT DELETE
    ON OBJECT::[dbo].[CM_RETENTION] TO [CORP\dmukherji]
    AS [dbo];

