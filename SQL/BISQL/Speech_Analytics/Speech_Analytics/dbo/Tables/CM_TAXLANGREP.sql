CREATE TABLE [dbo].[CM_TAXLANGREP] (
    [EOM]         DATE           NULL,
    [AGNTID]      NVARCHAR (100) NULL,
    [CALLID]      FLOAT (53)     NULL,
    [PHNUMB]      NVARCHAR (15)  NULL,
    [CALLDT]      DATE           NULL,
    [DIS1]        NVARCHAR (100) NULL,
    [CLDUR]       FLOAT (53)     NULL,
    [RGSSEID]     NVARCHAR (100) NULL,
    [RGSACC]      NVARCHAR (100) NULL,
    [DIR]         NVARCHAR (50)  NULL,
    [CLNTID]      NVARCHAR (200) NULL,
    [RECDP]       NVARCHAR (15)  NULL,
    [CALLCOUNT]   INT            NOT NULL,
    [WEEKNUMB]    VARCHAR (6)    NOT NULL,
    [AMTAXLANG]   INT            NOT NULL,
    [CXTAXLANG]   INT            NOT NULL,
    [FNLTAXLANG]  INT            NOT NULL,
    [NEGCALLS]    INT            NOT NULL,
    [SESSIONNAME] VARCHAR (100)  NULL
);




GO
GRANT UPDATE
    ON OBJECT::[dbo].[CM_TAXLANGREP] TO [CORP\musalunke]
    AS [dbo];


GO
GRANT SELECT
    ON OBJECT::[dbo].[CM_TAXLANGREP] TO [CORP\musalunke]
    AS [dbo];


GO
GRANT DELETE
    ON OBJECT::[dbo].[CM_TAXLANGREP] TO [CORP\musalunke]
    AS [dbo];


GO
GRANT UPDATE
    ON OBJECT::[dbo].[CM_TAXLANGREP] TO [CORP\pjain]
    AS [dbo];


GO
GRANT SELECT
    ON OBJECT::[dbo].[CM_TAXLANGREP] TO [CORP\pjain]
    AS [dbo];


GO
GRANT DELETE
    ON OBJECT::[dbo].[CM_TAXLANGREP] TO [CORP\pjain]
    AS [dbo];


GO
GRANT UPDATE
    ON OBJECT::[dbo].[CM_TAXLANGREP] TO [CORP\mhuang]
    AS [dbo];


GO
GRANT SELECT
    ON OBJECT::[dbo].[CM_TAXLANGREP] TO [CORP\mhuang]
    AS [dbo];


GO
GRANT DELETE
    ON OBJECT::[dbo].[CM_TAXLANGREP] TO [CORP\mhuang]
    AS [dbo];


GO
GRANT UPDATE
    ON OBJECT::[dbo].[CM_TAXLANGREP] TO [CORP\tkumar]
    AS [dbo];


GO
GRANT UPDATE
    ON OBJECT::[dbo].[CM_TAXLANGREP] TO [CORP\dmukherji]
    AS [dbo];


GO
GRANT UPDATE
    ON OBJECT::[dbo].[CM_TAXLANGREP] TO [CORP\aughodake]
    AS [dbo];


GO
GRANT UPDATE
    ON OBJECT::[dbo].[CM_TAXLANGREP] TO [CORP\aramugade]
    AS [dbo];


GO
GRANT SELECT
    ON OBJECT::[dbo].[CM_TAXLANGREP] TO [CORP\tkumar]
    AS [dbo];


GO
GRANT SELECT
    ON OBJECT::[dbo].[CM_TAXLANGREP] TO [CORP\dmukherji]
    AS [dbo];


GO
GRANT SELECT
    ON OBJECT::[dbo].[CM_TAXLANGREP] TO [CORP\aughodake]
    AS [dbo];


GO
GRANT SELECT
    ON OBJECT::[dbo].[CM_TAXLANGREP] TO [CORP\aramugade]
    AS [dbo];


GO
GRANT DELETE
    ON OBJECT::[dbo].[CM_TAXLANGREP] TO [CORP\tkumar]
    AS [dbo];


GO
GRANT DELETE
    ON OBJECT::[dbo].[CM_TAXLANGREP] TO [CORP\dmukherji]
    AS [dbo];


GO
GRANT DELETE
    ON OBJECT::[dbo].[CM_TAXLANGREP] TO [CORP\aughodake]
    AS [dbo];


GO
GRANT DELETE
    ON OBJECT::[dbo].[CM_TAXLANGREP] TO [CORP\aramugade]
    AS [dbo];

