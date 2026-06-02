CREATE TABLE [dbo].[CM_ATTOPNREP] (
    [EOM]          DATE           NULL,
    [AGNTID]       NVARCHAR (100) NULL,
    [CALLID]       FLOAT (53)     NULL,
    [CALLDT]       DATE           NULL,
    [RGSSEID]      NVARCHAR (100) NULL,
    [DIS1]         NVARCHAR (100) NULL,
    [RECDP]        NVARCHAR (15)  NULL,
    [PHNUMB]       NVARCHAR (15)  NULL,
    [CLNTID]       NVARCHAR (200) NULL,
    [SILT]         FLOAT (53)     NULL,
    [CLDUR]        FLOAT (53)     NULL,
    [DIS1CAT]      NVARCHAR (10)  NULL,
    [MRDTIME]      FLOAT (53)     NULL,
    [MMHIT]        INT            NULL,
    [MMTIME]       FLOAT (53)     NULL,
    [CRDHIT]       INT            NULL,
    [CRDTIME]      FLOAT (53)     NULL,
    [BALHIT]       INT            NULL,
    [BALTIME]      FLOAT (53)     NULL,
    [CALLCOUNT]    INT            NULL,
    [EXTRWFLG]     INT            NULL,
    [EXTRCOMPNAME] NVARCHAR (255) NULL,
    [EXTRWTIME]    FLOAT (53)     NULL,
    [CXOPNLANG]    NVARCHAR (150) NULL,
    [CXOPNCOMP]    NVARCHAR (255) NULL,
    [CXOPNTME]     FLOAT (53)     NULL,
    [TMEDIFFFNL]   FLOAT (53)     NULL,
    [CXFLG]        INT            NOT NULL,
    [CXFLGTXT]     VARCHAR (3)    NOT NULL,
    [ACTTTTME]     FLOAT (53)     NULL,
    [STRNBG]       VARCHAR (14)   NOT NULL,
    [WKNUMB]       VARCHAR (6)    NOT NULL
);




GO
GRANT UPDATE
    ON OBJECT::[dbo].[CM_ATTOPNREP] TO [CORP\musalunke]
    AS [dbo];


GO
GRANT SELECT
    ON OBJECT::[dbo].[CM_ATTOPNREP] TO [CORP\musalunke]
    AS [dbo];


GO
GRANT DELETE
    ON OBJECT::[dbo].[CM_ATTOPNREP] TO [CORP\musalunke]
    AS [dbo];


GO
GRANT UPDATE
    ON OBJECT::[dbo].[CM_ATTOPNREP] TO [CORP\pjain]
    AS [dbo];


GO
GRANT SELECT
    ON OBJECT::[dbo].[CM_ATTOPNREP] TO [CORP\pjain]
    AS [dbo];


GO
GRANT DELETE
    ON OBJECT::[dbo].[CM_ATTOPNREP] TO [CORP\pjain]
    AS [dbo];


GO
GRANT UPDATE
    ON OBJECT::[dbo].[CM_ATTOPNREP] TO [CORP\mhuang]
    AS [dbo];


GO
GRANT SELECT
    ON OBJECT::[dbo].[CM_ATTOPNREP] TO [CORP\mhuang]
    AS [dbo];


GO
GRANT DELETE
    ON OBJECT::[dbo].[CM_ATTOPNREP] TO [CORP\mhuang]
    AS [dbo];


GO
GRANT UPDATE
    ON OBJECT::[dbo].[CM_ATTOPNREP] TO [CORP\tkumar]
    AS [dbo];


GO
GRANT UPDATE
    ON OBJECT::[dbo].[CM_ATTOPNREP] TO [CORP\dmukherji]
    AS [dbo];


GO
GRANT UPDATE
    ON OBJECT::[dbo].[CM_ATTOPNREP] TO [CORP\aughodake]
    AS [dbo];


GO
GRANT UPDATE
    ON OBJECT::[dbo].[CM_ATTOPNREP] TO [CORP\aramugade]
    AS [dbo];


GO
GRANT SELECT
    ON OBJECT::[dbo].[CM_ATTOPNREP] TO [CORP\tkumar]
    AS [dbo];


GO
GRANT SELECT
    ON OBJECT::[dbo].[CM_ATTOPNREP] TO [CORP\dmukherji]
    AS [dbo];


GO
GRANT SELECT
    ON OBJECT::[dbo].[CM_ATTOPNREP] TO [CORP\aughodake]
    AS [dbo];


GO
GRANT SELECT
    ON OBJECT::[dbo].[CM_ATTOPNREP] TO [CORP\aramugade]
    AS [dbo];


GO
GRANT DELETE
    ON OBJECT::[dbo].[CM_ATTOPNREP] TO [CORP\tkumar]
    AS [dbo];


GO
GRANT DELETE
    ON OBJECT::[dbo].[CM_ATTOPNREP] TO [CORP\dmukherji]
    AS [dbo];


GO
GRANT DELETE
    ON OBJECT::[dbo].[CM_ATTOPNREP] TO [CORP\aughodake]
    AS [dbo];


GO
GRANT DELETE
    ON OBJECT::[dbo].[CM_ATTOPNREP] TO [CORP\aramugade]
    AS [dbo];

