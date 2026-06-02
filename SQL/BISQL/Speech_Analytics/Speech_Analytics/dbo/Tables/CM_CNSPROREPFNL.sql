CREATE TABLE [dbo].[CM_CNSPROREPFNL] (
    [EOM]      DATE            NULL,
    [AGNTID]   NVARCHAR (100)  NULL,
    [CALLID]   FLOAT (53)      NULL,
    [CALLDT]   DATE            NULL,
    [RECDP]    NVARCHAR (15)   NULL,
    [RGSACC]   NVARCHAR (100)  NULL,
    [DIS1]     NVARCHAR (100)  NULL,
    [SKNM]     NVARCHAR (100)  NULL,
    [CLDUR]    FLOAT (53)      NULL,
    [WRCNT]    FLOAT (53)      NULL,
    [DIR]      NVARCHAR (50)   NULL,
    [RGSSEID]  NVARCHAR (100)  NULL,
    [PHNUMB]   NVARCHAR (15)   NULL,
    [PAYRCVD]  FLOAT (53)      NULL,
    [CNS_CAT]  NVARCHAR (4000) NULL,
    [CNS_COMP] NVARCHAR (255)  NULL,
    [CNS_TIME] FLOAT (53)      NULL,
    [CLNT_ID]  VARCHAR (10)    NULL,
    [CLLCNT]   INT             NOT NULL,
    [PTP_FLAG] INT             NOT NULL,
    [CNSPITCH] INT             NULL,
    [CNSPROM]  INT             NULL,
    [CNSCONT]  INT             NULL,
    [CNSAVOI]  INT             NULL,
    [AMWRCNT]  INT             NULL,
    [CNWRCNT]  INT             NULL,
    [OPNQCNT]  INT             NULL,
    [CLOQCNT]  INT             NULL
);






GO
CREATE NONCLUSTERED INDEX [IX_CM_CNSPROREPFNL_CALLID]
    ON [dbo].[CM_CNSPROREPFNL]([CALLID] ASC) WITH (DATA_COMPRESSION = PAGE);


GO
GRANT UPDATE
    ON OBJECT::[dbo].[CM_CNSPROREPFNL] TO [CORP\tkumar]
    AS [dbo];


GO
GRANT UPDATE
    ON OBJECT::[dbo].[CM_CNSPROREPFNL] TO [CORP\pjain]
    AS [dbo];


GO
GRANT UPDATE
    ON OBJECT::[dbo].[CM_CNSPROREPFNL] TO [CORP\musalunke]
    AS [dbo];


GO
GRANT UPDATE
    ON OBJECT::[dbo].[CM_CNSPROREPFNL] TO [CORP\mhuang]
    AS [dbo];


GO
GRANT UPDATE
    ON OBJECT::[dbo].[CM_CNSPROREPFNL] TO [CORP\dmukherji]
    AS [dbo];


GO
GRANT UPDATE
    ON OBJECT::[dbo].[CM_CNSPROREPFNL] TO [CORP\aughodake]
    AS [dbo];


GO
GRANT UPDATE
    ON OBJECT::[dbo].[CM_CNSPROREPFNL] TO [CORP\aramugade]
    AS [dbo];


GO
GRANT SELECT
    ON OBJECT::[dbo].[CM_CNSPROREPFNL] TO [CORP\tkumar]
    AS [dbo];


GO
GRANT SELECT
    ON OBJECT::[dbo].[CM_CNSPROREPFNL] TO [CORP\pjain]
    AS [dbo];


GO
GRANT SELECT
    ON OBJECT::[dbo].[CM_CNSPROREPFNL] TO [CORP\musalunke]
    AS [dbo];


GO
GRANT SELECT
    ON OBJECT::[dbo].[CM_CNSPROREPFNL] TO [CORP\mhuang]
    AS [dbo];


GO
GRANT SELECT
    ON OBJECT::[dbo].[CM_CNSPROREPFNL] TO [CORP\dmukherji]
    AS [dbo];


GO
GRANT SELECT
    ON OBJECT::[dbo].[CM_CNSPROREPFNL] TO [CORP\aughodake]
    AS [dbo];


GO
GRANT SELECT
    ON OBJECT::[dbo].[CM_CNSPROREPFNL] TO [CORP\aramugade]
    AS [dbo];


GO
GRANT DELETE
    ON OBJECT::[dbo].[CM_CNSPROREPFNL] TO [CORP\tkumar]
    AS [dbo];


GO
GRANT DELETE
    ON OBJECT::[dbo].[CM_CNSPROREPFNL] TO [CORP\pjain]
    AS [dbo];


GO
GRANT DELETE
    ON OBJECT::[dbo].[CM_CNSPROREPFNL] TO [CORP\musalunke]
    AS [dbo];


GO
GRANT DELETE
    ON OBJECT::[dbo].[CM_CNSPROREPFNL] TO [CORP\mhuang]
    AS [dbo];


GO
GRANT DELETE
    ON OBJECT::[dbo].[CM_CNSPROREPFNL] TO [CORP\dmukherji]
    AS [dbo];


GO
GRANT DELETE
    ON OBJECT::[dbo].[CM_CNSPROREPFNL] TO [CORP\aughodake]
    AS [dbo];


GO
GRANT DELETE
    ON OBJECT::[dbo].[CM_CNSPROREPFNL] TO [CORP\aramugade]
    AS [dbo];

