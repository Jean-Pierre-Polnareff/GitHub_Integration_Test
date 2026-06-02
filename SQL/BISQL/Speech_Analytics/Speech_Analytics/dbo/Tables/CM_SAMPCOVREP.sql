CREATE TABLE [dbo].[CM_SAMPCOVREP] (
    [EOM]        DATE           NULL,
    [AGNTID]     NVARCHAR (100) NULL,
    [CALLDT]     DATE           NULL,
    [PSSBLECLLL] INT            NULL,
    [DNERPC250]  INT            NOT NULL,
    [DNERPC400]  INT            NOT NULL,
    [DNERPC600]  INT            NOT NULL,
    [DNEPTP]     INT            NOT NULL,
    [DNETHRDP]   INT            NOT NULL,
    [DNELM]      INT            NOT NULL,
    [TOTDNE]     INT            NULL
);




GO
GRANT UPDATE
    ON OBJECT::[dbo].[CM_SAMPCOVREP] TO [CORP\tkumar]
    AS [dbo];


GO
GRANT UPDATE
    ON OBJECT::[dbo].[CM_SAMPCOVREP] TO [CORP\pjain]
    AS [dbo];


GO
GRANT UPDATE
    ON OBJECT::[dbo].[CM_SAMPCOVREP] TO [CORP\musalunke]
    AS [dbo];


GO
GRANT UPDATE
    ON OBJECT::[dbo].[CM_SAMPCOVREP] TO [CORP\mhuang]
    AS [dbo];


GO
GRANT UPDATE
    ON OBJECT::[dbo].[CM_SAMPCOVREP] TO [CORP\dmukherji]
    AS [dbo];


GO
GRANT UPDATE
    ON OBJECT::[dbo].[CM_SAMPCOVREP] TO [CORP\aughodake]
    AS [dbo];


GO
GRANT UPDATE
    ON OBJECT::[dbo].[CM_SAMPCOVREP] TO [CORP\aramugade]
    AS [dbo];


GO
GRANT SELECT
    ON OBJECT::[dbo].[CM_SAMPCOVREP] TO [CORP\tkumar]
    AS [dbo];


GO
GRANT SELECT
    ON OBJECT::[dbo].[CM_SAMPCOVREP] TO [CORP\pjain]
    AS [dbo];


GO
GRANT SELECT
    ON OBJECT::[dbo].[CM_SAMPCOVREP] TO [CORP\musalunke]
    AS [dbo];


GO
GRANT SELECT
    ON OBJECT::[dbo].[CM_SAMPCOVREP] TO [CORP\mhuang]
    AS [dbo];


GO
GRANT SELECT
    ON OBJECT::[dbo].[CM_SAMPCOVREP] TO [CORP\dmukherji]
    AS [dbo];


GO
GRANT SELECT
    ON OBJECT::[dbo].[CM_SAMPCOVREP] TO [CORP\aughodake]
    AS [dbo];


GO
GRANT SELECT
    ON OBJECT::[dbo].[CM_SAMPCOVREP] TO [CORP\aramugade]
    AS [dbo];


GO
GRANT DELETE
    ON OBJECT::[dbo].[CM_SAMPCOVREP] TO [CORP\tkumar]
    AS [dbo];


GO
GRANT DELETE
    ON OBJECT::[dbo].[CM_SAMPCOVREP] TO [CORP\pjain]
    AS [dbo];


GO
GRANT DELETE
    ON OBJECT::[dbo].[CM_SAMPCOVREP] TO [CORP\musalunke]
    AS [dbo];


GO
GRANT DELETE
    ON OBJECT::[dbo].[CM_SAMPCOVREP] TO [CORP\mhuang]
    AS [dbo];


GO
GRANT DELETE
    ON OBJECT::[dbo].[CM_SAMPCOVREP] TO [CORP\dmukherji]
    AS [dbo];


GO
GRANT DELETE
    ON OBJECT::[dbo].[CM_SAMPCOVREP] TO [CORP\aughodake]
    AS [dbo];


GO
GRANT DELETE
    ON OBJECT::[dbo].[CM_SAMPCOVREP] TO [CORP\aramugade]
    AS [dbo];

