CREATE TABLE [dbo].[CM_REFVERBEHA] (
    [EOM]       DATE            NULL,
    [AGNTID]    NVARCHAR (100)  NULL,
    [CALLID]    FLOAT (53)      NULL,
    [CALLDT]    DATE            NULL,
    [DIR]       NVARCHAR (50)   NULL,
    [RECDP]     NVARCHAR (15)   NULL,
    [PHNUMB]    NVARCHAR (15)   NULL,
    [CLNTID]    NVARCHAR (200)  NULL,
    [SILT]      FLOAT (53)      NULL,
    [CLDUR]     FLOAT (53)      NULL,
    [DIS1]      NVARCHAR (100)  NULL,
    [RGSSEID]   NVARCHAR (4000) NULL,
    [RGSACC]    NVARCHAR (100)  NULL,
    [COMPNAME]  NVARCHAR (255)  NULL,
    [STTIME]    FLOAT (53)      NULL,
    [CALLCOUNT] INT             NOT NULL
);




GO
GRANT UPDATE
    ON OBJECT::[dbo].[CM_REFVERBEHA] TO [CORP\musalunke]
    AS [dbo];


GO
GRANT SELECT
    ON OBJECT::[dbo].[CM_REFVERBEHA] TO [CORP\musalunke]
    AS [dbo];


GO
GRANT DELETE
    ON OBJECT::[dbo].[CM_REFVERBEHA] TO [CORP\musalunke]
    AS [dbo];


GO
GRANT UPDATE
    ON OBJECT::[dbo].[CM_REFVERBEHA] TO [CORP\pjain]
    AS [dbo];


GO
GRANT SELECT
    ON OBJECT::[dbo].[CM_REFVERBEHA] TO [CORP\pjain]
    AS [dbo];


GO
GRANT DELETE
    ON OBJECT::[dbo].[CM_REFVERBEHA] TO [CORP\pjain]
    AS [dbo];


GO
GRANT UPDATE
    ON OBJECT::[dbo].[CM_REFVERBEHA] TO [CORP\mhuang]
    AS [dbo];


GO
GRANT SELECT
    ON OBJECT::[dbo].[CM_REFVERBEHA] TO [CORP\mhuang]
    AS [dbo];


GO
GRANT DELETE
    ON OBJECT::[dbo].[CM_REFVERBEHA] TO [CORP\mhuang]
    AS [dbo];


GO
GRANT UPDATE
    ON OBJECT::[dbo].[CM_REFVERBEHA] TO [CORP\tkumar]
    AS [dbo];


GO
GRANT UPDATE
    ON OBJECT::[dbo].[CM_REFVERBEHA] TO [CORP\dmukherji]
    AS [dbo];


GO
GRANT UPDATE
    ON OBJECT::[dbo].[CM_REFVERBEHA] TO [CORP\aughodake]
    AS [dbo];


GO
GRANT UPDATE
    ON OBJECT::[dbo].[CM_REFVERBEHA] TO [CORP\aramugade]
    AS [dbo];


GO
GRANT SELECT
    ON OBJECT::[dbo].[CM_REFVERBEHA] TO [CORP\tkumar]
    AS [dbo];


GO
GRANT SELECT
    ON OBJECT::[dbo].[CM_REFVERBEHA] TO [CORP\dmukherji]
    AS [dbo];


GO
GRANT SELECT
    ON OBJECT::[dbo].[CM_REFVERBEHA] TO [CORP\aughodake]
    AS [dbo];


GO
GRANT SELECT
    ON OBJECT::[dbo].[CM_REFVERBEHA] TO [CORP\aramugade]
    AS [dbo];


GO
GRANT DELETE
    ON OBJECT::[dbo].[CM_REFVERBEHA] TO [CORP\tkumar]
    AS [dbo];


GO
GRANT DELETE
    ON OBJECT::[dbo].[CM_REFVERBEHA] TO [CORP\dmukherji]
    AS [dbo];


GO
GRANT DELETE
    ON OBJECT::[dbo].[CM_REFVERBEHA] TO [CORP\aughodake]
    AS [dbo];


GO
GRANT DELETE
    ON OBJECT::[dbo].[CM_REFVERBEHA] TO [CORP\aramugade]
    AS [dbo];

