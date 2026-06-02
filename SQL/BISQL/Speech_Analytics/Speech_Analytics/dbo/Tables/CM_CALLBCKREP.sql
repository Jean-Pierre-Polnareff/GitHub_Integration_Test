CREATE TABLE [dbo].[CM_CALLBCKREP] (
    [EOM]      DATE           NULL,
    [AGNTID]   NVARCHAR (100) NULL,
    [CALLID]   FLOAT (53)     NULL,
    [CALLDT]   DATE           NULL,
    [RECDP]    NVARCHAR (15)  NULL,
    [RGSACC]   NVARCHAR (100) NULL,
    [DIS1]     NVARCHAR (100) NULL,
    [CLDUR]    FLOAT (53)     NULL,
    [PHNUMB]   NVARCHAR (15)  NULL,
    [CXREQST]  INT            NOT NULL,
    [CXSTTIME] FLOAT (53)     NULL,
    [AMPERM]   INT            NOT NULL,
    [AMSTTIME] FLOAT (53)     NULL,
    [YSTTIME]  FLOAT (53)     NULL,
    [YSUCC]    INT            NOT NULL,
    [CLLCNT]   INT            NOT NULL,
    [CLEXP]    INT            NOT NULL,
    [RGSSEID]  NVARCHAR (100) NULL
);






GO
CREATE NONCLUSTERED INDEX [IX_CM_CALLBCKREP_CALLID]
    ON [dbo].[CM_CALLBCKREP]([CALLID] ASC) WITH (DATA_COMPRESSION = PAGE);


GO
GRANT UPDATE
    ON OBJECT::[dbo].[CM_CALLBCKREP] TO [CORP\tkumar]
    AS [dbo];


GO
GRANT UPDATE
    ON OBJECT::[dbo].[CM_CALLBCKREP] TO [CORP\pjain]
    AS [dbo];


GO
GRANT UPDATE
    ON OBJECT::[dbo].[CM_CALLBCKREP] TO [CORP\musalunke]
    AS [dbo];


GO
GRANT UPDATE
    ON OBJECT::[dbo].[CM_CALLBCKREP] TO [CORP\mhuang]
    AS [dbo];


GO
GRANT UPDATE
    ON OBJECT::[dbo].[CM_CALLBCKREP] TO [CORP\dmukherji]
    AS [dbo];


GO
GRANT UPDATE
    ON OBJECT::[dbo].[CM_CALLBCKREP] TO [CORP\aughodake]
    AS [dbo];


GO
GRANT UPDATE
    ON OBJECT::[dbo].[CM_CALLBCKREP] TO [CORP\aramugade]
    AS [dbo];


GO
GRANT SELECT
    ON OBJECT::[dbo].[CM_CALLBCKREP] TO [CORP\tkumar]
    AS [dbo];


GO
GRANT SELECT
    ON OBJECT::[dbo].[CM_CALLBCKREP] TO [CORP\pjain]
    AS [dbo];


GO
GRANT SELECT
    ON OBJECT::[dbo].[CM_CALLBCKREP] TO [CORP\musalunke]
    AS [dbo];


GO
GRANT SELECT
    ON OBJECT::[dbo].[CM_CALLBCKREP] TO [CORP\mhuang]
    AS [dbo];


GO
GRANT SELECT
    ON OBJECT::[dbo].[CM_CALLBCKREP] TO [CORP\dmukherji]
    AS [dbo];


GO
GRANT SELECT
    ON OBJECT::[dbo].[CM_CALLBCKREP] TO [CORP\aughodake]
    AS [dbo];


GO
GRANT SELECT
    ON OBJECT::[dbo].[CM_CALLBCKREP] TO [CORP\aramugade]
    AS [dbo];


GO
GRANT DELETE
    ON OBJECT::[dbo].[CM_CALLBCKREP] TO [CORP\tkumar]
    AS [dbo];


GO
GRANT DELETE
    ON OBJECT::[dbo].[CM_CALLBCKREP] TO [CORP\pjain]
    AS [dbo];


GO
GRANT DELETE
    ON OBJECT::[dbo].[CM_CALLBCKREP] TO [CORP\musalunke]
    AS [dbo];


GO
GRANT DELETE
    ON OBJECT::[dbo].[CM_CALLBCKREP] TO [CORP\mhuang]
    AS [dbo];


GO
GRANT DELETE
    ON OBJECT::[dbo].[CM_CALLBCKREP] TO [CORP\dmukherji]
    AS [dbo];


GO
GRANT DELETE
    ON OBJECT::[dbo].[CM_CALLBCKREP] TO [CORP\aughodake]
    AS [dbo];


GO
GRANT DELETE
    ON OBJECT::[dbo].[CM_CALLBCKREP] TO [CORP\aramugade]
    AS [dbo];

