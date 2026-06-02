CREATE TABLE [dbo].[CM_NCALLBCKREP] (
    [EOM]        DATE           NULL,
    [AGNTID]     NVARCHAR (100) NULL,
    [CALLID]     FLOAT (53)     NULL,
    [CALLDT]     DATE           NULL,
    [RECDP]      NVARCHAR (15)  NULL,
    [RGSACC]     NVARCHAR (100) NULL,
    [DIS1]       NVARCHAR (100) NULL,
    [CLDUR]      FLOAT (53)     NULL,
    [PHNUMB]     NVARCHAR (15)  NULL,
    [RGSSEID]    NVARCHAR (100) NULL,
    [CXSTTIME]   FLOAT (53)     NULL,
    [CXLANG]     NVARCHAR (255) NULL,
    [CXLANGTIME] FLOAT (53)     NULL,
    [CXCATE]     VARCHAR (12)   NOT NULL
);






GO
CREATE NONCLUSTERED INDEX [IX_CM_NCALLBCKREP_CALLID]
    ON [dbo].[CM_NCALLBCKREP]([CALLID] ASC) WITH (DATA_COMPRESSION = PAGE);


GO
GRANT UPDATE
    ON OBJECT::[dbo].[CM_NCALLBCKREP] TO [CORP\tkumar]
    AS [dbo];


GO
GRANT UPDATE
    ON OBJECT::[dbo].[CM_NCALLBCKREP] TO [CORP\pjain]
    AS [dbo];


GO
GRANT UPDATE
    ON OBJECT::[dbo].[CM_NCALLBCKREP] TO [CORP\musalunke]
    AS [dbo];


GO
GRANT UPDATE
    ON OBJECT::[dbo].[CM_NCALLBCKREP] TO [CORP\mhuang]
    AS [dbo];


GO
GRANT UPDATE
    ON OBJECT::[dbo].[CM_NCALLBCKREP] TO [CORP\dmukherji]
    AS [dbo];


GO
GRANT UPDATE
    ON OBJECT::[dbo].[CM_NCALLBCKREP] TO [CORP\aughodake]
    AS [dbo];


GO
GRANT UPDATE
    ON OBJECT::[dbo].[CM_NCALLBCKREP] TO [CORP\aramugade]
    AS [dbo];


GO
GRANT SELECT
    ON OBJECT::[dbo].[CM_NCALLBCKREP] TO [CORP\tkumar]
    AS [dbo];


GO
GRANT SELECT
    ON OBJECT::[dbo].[CM_NCALLBCKREP] TO [CORP\pjain]
    AS [dbo];


GO
GRANT SELECT
    ON OBJECT::[dbo].[CM_NCALLBCKREP] TO [CORP\musalunke]
    AS [dbo];


GO
GRANT SELECT
    ON OBJECT::[dbo].[CM_NCALLBCKREP] TO [CORP\mhuang]
    AS [dbo];


GO
GRANT SELECT
    ON OBJECT::[dbo].[CM_NCALLBCKREP] TO [CORP\dmukherji]
    AS [dbo];


GO
GRANT SELECT
    ON OBJECT::[dbo].[CM_NCALLBCKREP] TO [CORP\aughodake]
    AS [dbo];


GO
GRANT SELECT
    ON OBJECT::[dbo].[CM_NCALLBCKREP] TO [CORP\aramugade]
    AS [dbo];


GO
GRANT DELETE
    ON OBJECT::[dbo].[CM_NCALLBCKREP] TO [CORP\tkumar]
    AS [dbo];


GO
GRANT DELETE
    ON OBJECT::[dbo].[CM_NCALLBCKREP] TO [CORP\pjain]
    AS [dbo];


GO
GRANT DELETE
    ON OBJECT::[dbo].[CM_NCALLBCKREP] TO [CORP\musalunke]
    AS [dbo];


GO
GRANT DELETE
    ON OBJECT::[dbo].[CM_NCALLBCKREP] TO [CORP\mhuang]
    AS [dbo];


GO
GRANT DELETE
    ON OBJECT::[dbo].[CM_NCALLBCKREP] TO [CORP\dmukherji]
    AS [dbo];


GO
GRANT DELETE
    ON OBJECT::[dbo].[CM_NCALLBCKREP] TO [CORP\aughodake]
    AS [dbo];


GO
GRANT DELETE
    ON OBJECT::[dbo].[CM_NCALLBCKREP] TO [CORP\aramugade]
    AS [dbo];

