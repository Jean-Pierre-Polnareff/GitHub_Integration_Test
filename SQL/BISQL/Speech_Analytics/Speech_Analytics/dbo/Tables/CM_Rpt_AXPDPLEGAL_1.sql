CREATE TABLE [dbo].[CM_Rpt_AXPDPLEGAL] (
    [EOM]            DATE            NULL,
    [AGNTID]         NVARCHAR (100)  NULL,
    [CALLID]         FLOAT (53)      NULL,
    [CALLDT]         DATE            NULL,
    [RGSSEID]        NVARCHAR (100)  NULL,
    [DIS1]           NVARCHAR (100)  NULL,
    [RECDP]          NVARCHAR (15)   NULL,
    [PHNUMB]         NVARCHAR (15)   NULL,
    [CLNTID]         NVARCHAR (200)  NULL,
    [SILT]           FLOAT (53)      NULL,
    [CLDUR]          FLOAT (53)      NULL,
    [CATCNT]         FLOAT (53)      NULL,
    [DIS1CAT]        NVARCHAR (10)   NULL,
    [OPNCAL]         FLOAT (53)      NULL,
    [CALLCOUNT]      INT             NOT NULL,
    [DPHIT]          INT             NULL,
    [DPTM]           FLOAT (53)      NULL,
    [SCNDVC]         INT             NULL,
    [SCNDVCTM]       FLOAT (53)      NULL,
    [CXCATHITAGNT]   NVARCHAR (255)  NULL,
    [CXCOMPNAMEAGNT] NVARCHAR (255)  NULL,
    [CXLANGTIMEAGNT] FLOAT (53)      NULL,
    [CXCATHITSUP]    NVARCHAR (255)  NULL,
    [CXCOMPNAMESUP]  NVARCHAR (255)  NULL,
    [CXLANGTIMESUP]  FLOAT (53)      NULL,
    [CXLANGAGNTGRP]  NVARCHAR (150)  NULL,
    [CXLANGSUPGRP]   NVARCHAR (150)  NULL,
    [WKNUMB]         VARCHAR (6)     NOT NULL,
    [STRNBG]         VARCHAR (14)    NOT NULL,
    [ACTMNTH]        NVARCHAR (4000) NULL,
    [DIR]            VARCHAR (50)    NULL,
    [RGSACC]         VARCHAR (100)   NULL,
    [COMPCOUNT]      INT             NULL
);


GO
GRANT UPDATE
    ON OBJECT::[dbo].[CM_Rpt_AXPDPLEGAL] TO [CORP\dmukherji]
    AS [dbo];


GO
GRANT SELECT
    ON OBJECT::[dbo].[CM_Rpt_AXPDPLEGAL] TO [CORP\dmukherji]
    AS [dbo];


GO
GRANT DELETE
    ON OBJECT::[dbo].[CM_Rpt_AXPDPLEGAL] TO [CORP\dmukherji]
    AS [dbo];


GO
GRANT SELECT
    ON OBJECT::[dbo].[CM_Rpt_AXPDPLEGAL] TO [CORP\tkumar]
    AS [dbo];


GO
GRANT SELECT
    ON OBJECT::[dbo].[CM_Rpt_AXPDPLEGAL] TO [corp\pdwivedi]
    AS [dbo];


GO
GRANT SELECT
    ON OBJECT::[dbo].[CM_Rpt_AXPDPLEGAL] TO [corp\ravijaykumar]
    AS [dbo];


GO
GRANT SELECT
    ON OBJECT::[dbo].[CM_Rpt_AXPDPLEGAL] TO [CORP\rsingh]
    AS [dbo];


GO
GRANT SELECT
    ON OBJECT::[dbo].[CM_Rpt_AXPDPLEGAL] TO [CORP\musalunke]
    AS [dbo];


GO
GRANT SELECT
    ON OBJECT::[dbo].[CM_Rpt_AXPDPLEGAL] TO [CORP\apoddar]
    AS [dbo];


GO
GRANT SELECT
    ON OBJECT::[dbo].[CM_Rpt_AXPDPLEGAL] TO [CORP\pjain]
    AS [dbo];

