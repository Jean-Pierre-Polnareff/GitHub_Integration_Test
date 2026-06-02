CREATE TABLE [dbo].[CM_NONRPCREPORT] (
    [EOM]     DATE           NULL,
    [AGNTID]  NVARCHAR (100) NULL,
    [CALLID]  FLOAT (53)     NULL,
    [PHNUMB]  NVARCHAR (15)  NULL,
    [CALLDT]  DATE           NULL,
    [DIS1]    NVARCHAR (100) NULL,
    [CLDUR]   FLOAT (53)     NULL,
    [RGSSEID] NVARCHAR (100) NULL,
    [RGSACC]  NVARCHAR (100) NULL,
    [DIR]     NVARCHAR (50)  NULL,
    [CLNTID]  NVARCHAR (200) NULL
);




GO
GRANT UPDATE
    ON OBJECT::[dbo].[CM_NONRPCREPORT] TO [CORP\musalunke]
    AS [dbo];


GO
GRANT SELECT
    ON OBJECT::[dbo].[CM_NONRPCREPORT] TO [CORP\musalunke]
    AS [dbo];


GO
GRANT DELETE
    ON OBJECT::[dbo].[CM_NONRPCREPORT] TO [CORP\musalunke]
    AS [dbo];


GO
GRANT UPDATE
    ON OBJECT::[dbo].[CM_NONRPCREPORT] TO [CORP\pjain]
    AS [dbo];


GO
GRANT SELECT
    ON OBJECT::[dbo].[CM_NONRPCREPORT] TO [CORP\pjain]
    AS [dbo];


GO
GRANT DELETE
    ON OBJECT::[dbo].[CM_NONRPCREPORT] TO [CORP\pjain]
    AS [dbo];


GO
GRANT UPDATE
    ON OBJECT::[dbo].[CM_NONRPCREPORT] TO [CORP\mhuang]
    AS [dbo];


GO
GRANT SELECT
    ON OBJECT::[dbo].[CM_NONRPCREPORT] TO [CORP\mhuang]
    AS [dbo];


GO
GRANT DELETE
    ON OBJECT::[dbo].[CM_NONRPCREPORT] TO [CORP\mhuang]
    AS [dbo];


GO
GRANT UPDATE
    ON OBJECT::[dbo].[CM_NONRPCREPORT] TO [CORP\tkumar]
    AS [dbo];


GO
GRANT UPDATE
    ON OBJECT::[dbo].[CM_NONRPCREPORT] TO [CORP\dmukherji]
    AS [dbo];


GO
GRANT UPDATE
    ON OBJECT::[dbo].[CM_NONRPCREPORT] TO [CORP\aughodake]
    AS [dbo];


GO
GRANT UPDATE
    ON OBJECT::[dbo].[CM_NONRPCREPORT] TO [CORP\aramugade]
    AS [dbo];


GO
GRANT SELECT
    ON OBJECT::[dbo].[CM_NONRPCREPORT] TO [CORP\tkumar]
    AS [dbo];


GO
GRANT SELECT
    ON OBJECT::[dbo].[CM_NONRPCREPORT] TO [CORP\dmukherji]
    AS [dbo];


GO
GRANT SELECT
    ON OBJECT::[dbo].[CM_NONRPCREPORT] TO [CORP\aughodake]
    AS [dbo];


GO
GRANT SELECT
    ON OBJECT::[dbo].[CM_NONRPCREPORT] TO [CORP\aramugade]
    AS [dbo];


GO
GRANT DELETE
    ON OBJECT::[dbo].[CM_NONRPCREPORT] TO [CORP\tkumar]
    AS [dbo];


GO
GRANT DELETE
    ON OBJECT::[dbo].[CM_NONRPCREPORT] TO [CORP\dmukherji]
    AS [dbo];


GO
GRANT DELETE
    ON OBJECT::[dbo].[CM_NONRPCREPORT] TO [CORP\aughodake]
    AS [dbo];


GO
GRANT DELETE
    ON OBJECT::[dbo].[CM_NONRPCREPORT] TO [CORP\aramugade]
    AS [dbo];

