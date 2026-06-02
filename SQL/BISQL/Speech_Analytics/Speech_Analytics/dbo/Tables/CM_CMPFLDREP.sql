CREATE TABLE [dbo].[CM_CMPFLDREP] (
    [EOM]      DATE           NULL,
    [AGNTID]   NVARCHAR (100) NULL,
    [CALLID]   FLOAT (53)     NULL,
    [CALLDT]   DATE           NULL,
    [RECDP]    NVARCHAR (15)  NULL,
    [RGSACC]   NVARCHAR (100) NULL,
    [DIS1]     NVARCHAR (100) NULL,
    [SKNM]     NVARCHAR (100) NULL,
    [CLDUR]    FLOAT (53)     NULL,
    [RGSSEID]  NVARCHAR (100) NULL,
    [PHNUMB]   NVARCHAR (15)  NULL,
    [DIS1CAT]  NVARCHAR (50)  NULL,
    [CallCDe]  VARCHAR (75)   NULL,
    [DIR]      NVARCHAR (50)  NULL,
    [CALLCNT]  INT            NOT NULL,
    [MRDHIT3]  INT            NOT NULL,
    [MMHIT1]   INT            NOT NULL,
    [CRDHIT1]  INT            NOT NULL,
    [BALHIT]   INT            NOT NULL,
    [STTIME]   FLOAT (53)     NULL,
    [MRDOPP]   INT            NOT NULL,
    [MMOPP]    INT            NOT NULL,
    [CRDOPP]   INT            NOT NULL,
    [BALOPP]   INT            NOT NULL,
    [INCTR]    INT            NOT NULL,
    [ClientId] VARCHAR (100)  NULL
);




GO
GRANT UPDATE
    ON OBJECT::[dbo].[CM_CMPFLDREP] TO [CORP\tkumar]
    AS [dbo];


GO
GRANT UPDATE
    ON OBJECT::[dbo].[CM_CMPFLDREP] TO [CORP\pjain]
    AS [dbo];


GO
GRANT UPDATE
    ON OBJECT::[dbo].[CM_CMPFLDREP] TO [CORP\musalunke]
    AS [dbo];


GO
GRANT UPDATE
    ON OBJECT::[dbo].[CM_CMPFLDREP] TO [CORP\mhuang]
    AS [dbo];


GO
GRANT UPDATE
    ON OBJECT::[dbo].[CM_CMPFLDREP] TO [CORP\dmukherji]
    AS [dbo];


GO
GRANT UPDATE
    ON OBJECT::[dbo].[CM_CMPFLDREP] TO [CORP\aughodake]
    AS [dbo];


GO
GRANT UPDATE
    ON OBJECT::[dbo].[CM_CMPFLDREP] TO [CORP\aramugade]
    AS [dbo];


GO
GRANT SELECT
    ON OBJECT::[dbo].[CM_CMPFLDREP] TO [CORP\tkumar]
    AS [dbo];


GO
GRANT SELECT
    ON OBJECT::[dbo].[CM_CMPFLDREP] TO [CORP\pjain]
    AS [dbo];


GO
GRANT SELECT
    ON OBJECT::[dbo].[CM_CMPFLDREP] TO [CORP\musalunke]
    AS [dbo];


GO
GRANT SELECT
    ON OBJECT::[dbo].[CM_CMPFLDREP] TO [CORP\mhuang]
    AS [dbo];


GO
GRANT SELECT
    ON OBJECT::[dbo].[CM_CMPFLDREP] TO [CORP\dmukherji]
    AS [dbo];


GO
GRANT SELECT
    ON OBJECT::[dbo].[CM_CMPFLDREP] TO [CORP\aughodake]
    AS [dbo];


GO
GRANT SELECT
    ON OBJECT::[dbo].[CM_CMPFLDREP] TO [CORP\aramugade]
    AS [dbo];


GO
GRANT DELETE
    ON OBJECT::[dbo].[CM_CMPFLDREP] TO [CORP\tkumar]
    AS [dbo];


GO
GRANT DELETE
    ON OBJECT::[dbo].[CM_CMPFLDREP] TO [CORP\pjain]
    AS [dbo];


GO
GRANT DELETE
    ON OBJECT::[dbo].[CM_CMPFLDREP] TO [CORP\musalunke]
    AS [dbo];


GO
GRANT DELETE
    ON OBJECT::[dbo].[CM_CMPFLDREP] TO [CORP\mhuang]
    AS [dbo];


GO
GRANT DELETE
    ON OBJECT::[dbo].[CM_CMPFLDREP] TO [CORP\dmukherji]
    AS [dbo];


GO
GRANT DELETE
    ON OBJECT::[dbo].[CM_CMPFLDREP] TO [CORP\aughodake]
    AS [dbo];


GO
GRANT DELETE
    ON OBJECT::[dbo].[CM_CMPFLDREP] TO [CORP\aramugade]
    AS [dbo];

