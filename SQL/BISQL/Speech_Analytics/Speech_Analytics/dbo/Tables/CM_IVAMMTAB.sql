CREATE TABLE [dbo].[CM_IVAMMTAB] (
    [TITLE]    VARCHAR (5)    NOT NULL,
    [AGNTID]   NVARCHAR (100) NULL,
    [CALLID]   FLOAT (53)     NULL,
    [RECDP]    NVARCHAR (15)  NULL,
    [RGSACC]   NVARCHAR (100) NULL,
    [DIS1]     NVARCHAR (100) NULL,
    [SKNM]     NVARCHAR (100) NULL,
    [SILT]     FLOAT (53)     NULL,
    [CLDUR]    FLOAT (53)     NULL,
    [PHNUMB]   NVARCHAR (15)  NULL,
    [MMG_FLG]  INT            NULL,
    [MMINCDIS] VARCHAR (9)    NOT NULL,
    [EOM]      DATE           NULL,
    [CALLDT]   DATE           NULL
);




GO
CREATE NONCLUSTERED INDEX [IX_CM_IVAMMTAB_CALLID]
    ON [dbo].[CM_IVAMMTAB]([CALLID] ASC) WITH (DATA_COMPRESSION = PAGE);


GO
GRANT UPDATE
    ON OBJECT::[dbo].[CM_IVAMMTAB] TO [CORP\tkumar]
    AS [dbo];


GO
GRANT UPDATE
    ON OBJECT::[dbo].[CM_IVAMMTAB] TO [CORP\pjain]
    AS [dbo];


GO
GRANT UPDATE
    ON OBJECT::[dbo].[CM_IVAMMTAB] TO [CORP\musalunke]
    AS [dbo];


GO
GRANT UPDATE
    ON OBJECT::[dbo].[CM_IVAMMTAB] TO [CORP\mhuang]
    AS [dbo];


GO
GRANT UPDATE
    ON OBJECT::[dbo].[CM_IVAMMTAB] TO [CORP\dmukherji]
    AS [dbo];


GO
GRANT UPDATE
    ON OBJECT::[dbo].[CM_IVAMMTAB] TO [CORP\aughodake]
    AS [dbo];


GO
GRANT UPDATE
    ON OBJECT::[dbo].[CM_IVAMMTAB] TO [CORP\aramugade]
    AS [dbo];


GO
GRANT SELECT
    ON OBJECT::[dbo].[CM_IVAMMTAB] TO [CORP\tkumar]
    AS [dbo];


GO
GRANT SELECT
    ON OBJECT::[dbo].[CM_IVAMMTAB] TO [CORP\pjain]
    AS [dbo];


GO
GRANT SELECT
    ON OBJECT::[dbo].[CM_IVAMMTAB] TO [CORP\musalunke]
    AS [dbo];


GO
GRANT SELECT
    ON OBJECT::[dbo].[CM_IVAMMTAB] TO [CORP\mhuang]
    AS [dbo];


GO
GRANT SELECT
    ON OBJECT::[dbo].[CM_IVAMMTAB] TO [CORP\dmukherji]
    AS [dbo];


GO
GRANT SELECT
    ON OBJECT::[dbo].[CM_IVAMMTAB] TO [CORP\aughodake]
    AS [dbo];


GO
GRANT SELECT
    ON OBJECT::[dbo].[CM_IVAMMTAB] TO [CORP\aramugade]
    AS [dbo];


GO
GRANT DELETE
    ON OBJECT::[dbo].[CM_IVAMMTAB] TO [CORP\tkumar]
    AS [dbo];


GO
GRANT DELETE
    ON OBJECT::[dbo].[CM_IVAMMTAB] TO [CORP\pjain]
    AS [dbo];


GO
GRANT DELETE
    ON OBJECT::[dbo].[CM_IVAMMTAB] TO [CORP\musalunke]
    AS [dbo];


GO
GRANT DELETE
    ON OBJECT::[dbo].[CM_IVAMMTAB] TO [CORP\mhuang]
    AS [dbo];


GO
GRANT DELETE
    ON OBJECT::[dbo].[CM_IVAMMTAB] TO [CORP\dmukherji]
    AS [dbo];


GO
GRANT DELETE
    ON OBJECT::[dbo].[CM_IVAMMTAB] TO [CORP\aughodake]
    AS [dbo];


GO
GRANT DELETE
    ON OBJECT::[dbo].[CM_IVAMMTAB] TO [CORP\aramugade]
    AS [dbo];

