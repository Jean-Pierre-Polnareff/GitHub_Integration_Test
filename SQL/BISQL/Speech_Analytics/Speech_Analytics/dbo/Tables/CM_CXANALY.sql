CREATE TABLE [dbo].[CM_CXANALY] (
    [EOM]         DATE           NULL,
    [AGNTID]      NVARCHAR (100) NULL,
    [CALLID]      FLOAT (53)     NULL,
    [CALLDT]      DATE           NULL,
    [DIR]         NVARCHAR (50)  NULL,
    [CLNTID]      NVARCHAR (200) NULL,
    [SILT]        FLOAT (53)     NULL,
    [CLDUR]       FLOAT (53)     NULL,
    [DIS1CAT]     NVARCHAR (10)  NULL,
    [FSTBEHA]     NVARCHAR (255) NULL,
    [FSTBEHATME]  FLOAT (53)     NULL,
    [AGNTCAT]     VARCHAR (16)   NOT NULL,
    [AGNTCOMP]    NVARCHAR (255) NOT NULL,
    [AGNTRESPTME] FLOAT (53)     NULL,
    [CLLCNT]      INT            NOT NULL,
    [PTP_RNG]     INT            NOT NULL,
    [STRNBG]      VARCHAR (14)   NOT NULL,
    [WKNUMB]      VARCHAR (6)    NOT NULL,
    [BEHACAT]     VARCHAR (14)   NOT NULL,
    [PARTCAT]     VARCHAR (11)   NOT NULL,
    [STRMCAT]     VARCHAR (16)   NOT NULL,
    [RECDP]       NVARCHAR (15)  NULL,
    [SESSIONNAME] NVARCHAR (250) NULL
);




GO
GRANT UPDATE
    ON OBJECT::[dbo].[CM_CXANALY] TO [CORP\musalunke]
    AS [dbo];


GO
GRANT SELECT
    ON OBJECT::[dbo].[CM_CXANALY] TO [CORP\musalunke]
    AS [dbo];


GO
GRANT DELETE
    ON OBJECT::[dbo].[CM_CXANALY] TO [CORP\musalunke]
    AS [dbo];


GO
GRANT UPDATE
    ON OBJECT::[dbo].[CM_CXANALY] TO [CORP\pjain]
    AS [dbo];


GO
GRANT SELECT
    ON OBJECT::[dbo].[CM_CXANALY] TO [CORP\pjain]
    AS [dbo];


GO
GRANT DELETE
    ON OBJECT::[dbo].[CM_CXANALY] TO [CORP\pjain]
    AS [dbo];


GO
GRANT UPDATE
    ON OBJECT::[dbo].[CM_CXANALY] TO [CORP\mhuang]
    AS [dbo];


GO
GRANT SELECT
    ON OBJECT::[dbo].[CM_CXANALY] TO [CORP\mhuang]
    AS [dbo];


GO
GRANT DELETE
    ON OBJECT::[dbo].[CM_CXANALY] TO [CORP\mhuang]
    AS [dbo];


GO
GRANT UPDATE
    ON OBJECT::[dbo].[CM_CXANALY] TO [CORP\tkumar]
    AS [dbo];


GO
GRANT UPDATE
    ON OBJECT::[dbo].[CM_CXANALY] TO [CORP\dmukherji]
    AS [dbo];


GO
GRANT UPDATE
    ON OBJECT::[dbo].[CM_CXANALY] TO [CORP\aughodake]
    AS [dbo];


GO
GRANT UPDATE
    ON OBJECT::[dbo].[CM_CXANALY] TO [CORP\aramugade]
    AS [dbo];


GO
GRANT SELECT
    ON OBJECT::[dbo].[CM_CXANALY] TO [CORP\tkumar]
    AS [dbo];


GO
GRANT SELECT
    ON OBJECT::[dbo].[CM_CXANALY] TO [CORP\dmukherji]
    AS [dbo];


GO
GRANT SELECT
    ON OBJECT::[dbo].[CM_CXANALY] TO [CORP\aughodake]
    AS [dbo];


GO
GRANT SELECT
    ON OBJECT::[dbo].[CM_CXANALY] TO [CORP\aramugade]
    AS [dbo];


GO
GRANT DELETE
    ON OBJECT::[dbo].[CM_CXANALY] TO [CORP\tkumar]
    AS [dbo];


GO
GRANT DELETE
    ON OBJECT::[dbo].[CM_CXANALY] TO [CORP\dmukherji]
    AS [dbo];


GO
GRANT DELETE
    ON OBJECT::[dbo].[CM_CXANALY] TO [CORP\aughodake]
    AS [dbo];


GO
GRANT DELETE
    ON OBJECT::[dbo].[CM_CXANALY] TO [CORP\aramugade]
    AS [dbo];

