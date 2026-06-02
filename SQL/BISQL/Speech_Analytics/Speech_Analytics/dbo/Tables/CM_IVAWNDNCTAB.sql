CREATE TABLE [dbo].[CM_IVAWNDNCTAB] (
    [TITLE]    VARCHAR (8)    NOT NULL,
    [AGNTID]   NVARCHAR (100) NULL,
    [CALLID]   FLOAT (53)     NULL,
    [RGSACC]   NVARCHAR (100) NULL,
    [RECDP]    NVARCHAR (15)  NULL,
    [DIS1]     NVARCHAR (100) NULL,
    [SKNM]     NVARCHAR (100) NULL,
    [SILT]     FLOAT (53)     NULL,
    [CLDUR]    FLOAT (53)     NULL,
    [PHNUMB]   NVARCHAR (15)  NULL,
    [DED_FLG]  INT            NULL,
    [DNC_FLG]  INT            NULL,
    [WNG_FLG]  INT            NULL,
    [WNINCDIS] VARCHAR (9)    NOT NULL,
    [CLLCNT]   INT            NOT NULL,
    [EOM]      DATE           NULL,
    [CALLDT]   DATE           NULL
);




GO
CREATE NONCLUSTERED INDEX [IX_CM_IVAWNDNCTAB_CALLID]
    ON [dbo].[CM_IVAWNDNCTAB]([CALLID] ASC) WITH (DATA_COMPRESSION = PAGE);


GO
GRANT UPDATE
    ON OBJECT::[dbo].[CM_IVAWNDNCTAB] TO [CORP\tkumar]
    AS [dbo];


GO
GRANT UPDATE
    ON OBJECT::[dbo].[CM_IVAWNDNCTAB] TO [CORP\pjain]
    AS [dbo];


GO
GRANT UPDATE
    ON OBJECT::[dbo].[CM_IVAWNDNCTAB] TO [CORP\musalunke]
    AS [dbo];


GO
GRANT UPDATE
    ON OBJECT::[dbo].[CM_IVAWNDNCTAB] TO [CORP\mhuang]
    AS [dbo];


GO
GRANT UPDATE
    ON OBJECT::[dbo].[CM_IVAWNDNCTAB] TO [CORP\dmukherji]
    AS [dbo];


GO
GRANT UPDATE
    ON OBJECT::[dbo].[CM_IVAWNDNCTAB] TO [CORP\aughodake]
    AS [dbo];


GO
GRANT UPDATE
    ON OBJECT::[dbo].[CM_IVAWNDNCTAB] TO [CORP\aramugade]
    AS [dbo];


GO
GRANT SELECT
    ON OBJECT::[dbo].[CM_IVAWNDNCTAB] TO [CORP\tkumar]
    AS [dbo];


GO
GRANT SELECT
    ON OBJECT::[dbo].[CM_IVAWNDNCTAB] TO [CORP\pjain]
    AS [dbo];


GO
GRANT SELECT
    ON OBJECT::[dbo].[CM_IVAWNDNCTAB] TO [CORP\musalunke]
    AS [dbo];


GO
GRANT SELECT
    ON OBJECT::[dbo].[CM_IVAWNDNCTAB] TO [CORP\mhuang]
    AS [dbo];


GO
GRANT SELECT
    ON OBJECT::[dbo].[CM_IVAWNDNCTAB] TO [CORP\dmukherji]
    AS [dbo];


GO
GRANT SELECT
    ON OBJECT::[dbo].[CM_IVAWNDNCTAB] TO [CORP\aughodake]
    AS [dbo];


GO
GRANT SELECT
    ON OBJECT::[dbo].[CM_IVAWNDNCTAB] TO [CORP\aramugade]
    AS [dbo];


GO
GRANT DELETE
    ON OBJECT::[dbo].[CM_IVAWNDNCTAB] TO [CORP\tkumar]
    AS [dbo];


GO
GRANT DELETE
    ON OBJECT::[dbo].[CM_IVAWNDNCTAB] TO [CORP\pjain]
    AS [dbo];


GO
GRANT DELETE
    ON OBJECT::[dbo].[CM_IVAWNDNCTAB] TO [CORP\musalunke]
    AS [dbo];


GO
GRANT DELETE
    ON OBJECT::[dbo].[CM_IVAWNDNCTAB] TO [CORP\mhuang]
    AS [dbo];


GO
GRANT DELETE
    ON OBJECT::[dbo].[CM_IVAWNDNCTAB] TO [CORP\dmukherji]
    AS [dbo];


GO
GRANT DELETE
    ON OBJECT::[dbo].[CM_IVAWNDNCTAB] TO [CORP\aughodake]
    AS [dbo];


GO
GRANT DELETE
    ON OBJECT::[dbo].[CM_IVAWNDNCTAB] TO [CORP\aramugade]
    AS [dbo];

