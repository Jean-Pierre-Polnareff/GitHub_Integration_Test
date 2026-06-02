CREATE TABLE [dbo].[CM_FSTPRTYSCORE_DLC] (
    [EOM]         DATE            NULL,
    [AGNTID]      NVARCHAR (100)  NULL,
    [CALLID]      FLOAT (53)      NULL,
    [CALLDT]      DATETIME        NULL,
    [DIS1]        NVARCHAR (100)  NULL,
    [SILT]        FLOAT (53)      NULL,
    [CLDUR]       FLOAT (53)      NULL,
    [DIR]         NVARCHAR (50)   NULL,
    [RGSSEID]     NVARCHAR (4000) NULL,
    [CDRESON]     NVARCHAR (255)  NULL,
    [CDSTTIME]    FLOAT (53)      NULL,
    [PLANG]       NVARCHAR (255)  NULL,
    [PSTTIME]     FLOAT (53)      NULL,
    [NLANG]       NVARCHAR (255)  NULL,
    [NSTTIME]     FLOAT (53)      NULL,
    [OWSCR]       INT             NOT NULL,
    [OWOPP]       INT             NOT NULL,
    [PSCR]        INT             NOT NULL,
    [POPP]        INT             NOT NULL,
    [CSMISCNT]    INT             NOT NULL,
    [HOLDABS]     INT             NOT NULL,
    [HOLDCNT]     INT             NOT NULL,
    [HLDSTIME]    FLOAT (53)      NOT NULL,
    [HLDETIME]    FLOAT (53)      NOT NULL,
    [PHRS]        NVARCHAR (4000) NOT NULL,
    [SILPER]      FLOAT (53)      NULL,
    [HLDSCR]      INT             NOT NULL,
    [EBSCR]       INT             NOT NULL,
    [EBILLSTTIME] FLOAT (53)      NULL,
    [EBOPP]       INT             NOT NULL,
    [VPARMSCR]    INT             NOT NULL,
    [VPAROPP]     INT             NOT NULL,
    [VPARMSCRFNL] INT             NOT NULL,
    [EBSCRFNL]    INT             NOT NULL,
    [PSCRFNL]     INT             NOT NULL,
    [OWSCRFNL]    INT             NOT NULL,
    [CXMISQUART]  NUMERIC (3, 2)  NULL,
    [SILPERQUART] NUMERIC (3, 2)  NULL,
    [FNLSCR]      NUMERIC (19, 3) NULL,
    [CLLCOUNT]    INT             NOT NULL,
    [SATISSCR]    INT             NULL,
    [NSATISSCR]   INT             NULL,
    [SATSOPP]     INT             NULL,
    [SATSSCRFNL]  INT             NULL,
    [NLANGCNT]    FLOAT (53)      NULL,
    [NOWNLANG]    NVARCHAR (255)  NULL,
    [NOWNSTTIME]  FLOAT (53)      NULL,
    [PHNUMB]      NVARCHAR (15)   NULL
);






GO
GRANT UPDATE
    ON OBJECT::[dbo].[CM_FSTPRTYSCORE_DLC] TO [CORP\musalunke]
    AS [dbo];


GO
GRANT SELECT
    ON OBJECT::[dbo].[CM_FSTPRTYSCORE_DLC] TO [CORP\musalunke]
    AS [dbo];


GO
GRANT DELETE
    ON OBJECT::[dbo].[CM_FSTPRTYSCORE_DLC] TO [CORP\musalunke]
    AS [dbo];


GO
GRANT UPDATE
    ON OBJECT::[dbo].[CM_FSTPRTYSCORE_DLC] TO [CORP\pjain]
    AS [dbo];


GO
GRANT SELECT
    ON OBJECT::[dbo].[CM_FSTPRTYSCORE_DLC] TO [CORP\pjain]
    AS [dbo];


GO
GRANT DELETE
    ON OBJECT::[dbo].[CM_FSTPRTYSCORE_DLC] TO [CORP\pjain]
    AS [dbo];


GO
GRANT UPDATE
    ON OBJECT::[dbo].[CM_FSTPRTYSCORE_DLC] TO [CORP\mhuang]
    AS [dbo];


GO
GRANT SELECT
    ON OBJECT::[dbo].[CM_FSTPRTYSCORE_DLC] TO [CORP\mhuang]
    AS [dbo];


GO
GRANT DELETE
    ON OBJECT::[dbo].[CM_FSTPRTYSCORE_DLC] TO [CORP\mhuang]
    AS [dbo];


GO
GRANT UPDATE
    ON OBJECT::[dbo].[CM_FSTPRTYSCORE_DLC] TO [CORP\tkumar]
    AS [dbo];


GO
GRANT UPDATE
    ON OBJECT::[dbo].[CM_FSTPRTYSCORE_DLC] TO [CORP\dmukherji]
    AS [dbo];


GO
GRANT UPDATE
    ON OBJECT::[dbo].[CM_FSTPRTYSCORE_DLC] TO [CORP\aughodake]
    AS [dbo];


GO
GRANT UPDATE
    ON OBJECT::[dbo].[CM_FSTPRTYSCORE_DLC] TO [CORP\aramugade]
    AS [dbo];


GO
GRANT SELECT
    ON OBJECT::[dbo].[CM_FSTPRTYSCORE_DLC] TO [CORP\tkumar]
    AS [dbo];


GO
GRANT SELECT
    ON OBJECT::[dbo].[CM_FSTPRTYSCORE_DLC] TO [CORP\dmukherji]
    AS [dbo];


GO
GRANT SELECT
    ON OBJECT::[dbo].[CM_FSTPRTYSCORE_DLC] TO [CORP\aughodake]
    AS [dbo];


GO
GRANT SELECT
    ON OBJECT::[dbo].[CM_FSTPRTYSCORE_DLC] TO [CORP\aramugade]
    AS [dbo];


GO
GRANT DELETE
    ON OBJECT::[dbo].[CM_FSTPRTYSCORE_DLC] TO [CORP\tkumar]
    AS [dbo];


GO
GRANT DELETE
    ON OBJECT::[dbo].[CM_FSTPRTYSCORE_DLC] TO [CORP\dmukherji]
    AS [dbo];


GO
GRANT DELETE
    ON OBJECT::[dbo].[CM_FSTPRTYSCORE_DLC] TO [CORP\aughodake]
    AS [dbo];


GO
GRANT DELETE
    ON OBJECT::[dbo].[CM_FSTPRTYSCORE_DLC] TO [CORP\aramugade]
    AS [dbo];

