CREATE TABLE [dbo].[CM_RPT_SecondVoice] (
    [EOM]              DATE           NULL,
    [AGNTID]           NVARCHAR (100) NULL,
    [CALLID]           FLOAT (53)     NULL,
    [CALLDT]           DATE           NULL,
    [RGSSEID]          NVARCHAR (100) NULL,
    [DIS1]             NVARCHAR (100) NULL,
    [RECDP]            NVARCHAR (15)  NULL,
    [PHNUMB]           NVARCHAR (15)  NULL,
    [CLNTID]           NVARCHAR (200) NULL,
    [SILT]             FLOAT (53)     NULL,
    [CLDUR]            FLOAT (53)     NULL,
    [CATCNT]           FLOAT (53)     NULL,
    [DIS1CAT]          NVARCHAR (10)  NULL,
    [OPNCAL]           FLOAT (53)     NULL,
    [CALLCOUNT]        INT            NOT NULL,
    [SCNDVC]           INT            NOT NULL,
    [SCNDVCTM]         FLOAT (53)     NULL,
    [CXCATHITAGNT]     NVARCHAR (255) NULL,
    [CXCOMPNAMEAGNT]   NVARCHAR (255) NULL,
    [CXLANGTIMEAGNT]   FLOAT (53)     NULL,
    [CXCATHITSUP]      NVARCHAR (255) NULL,
    [CXCOMPNAMESUP]    NVARCHAR (255) NULL,
    [CXLANGTIMESUP]    FLOAT (53)     NULL,
    [CXLANGAGNTGRP]    NVARCHAR (150) NULL,
    [CXLANGSUPGRP]     NVARCHAR (150) NULL,
    [ACTTALKTIME]      FLOAT (53)     NULL,
    [SUPTIME]          FLOAT (53)     NULL,
    [AGNTTIME]         FLOAT (53)     NULL,
    [PTP_Flags]        INT            NOT NULL,
    [SCNDVC_Flags]     VARCHAR (3)    NOT NULL,
    [BEHA_Change_Flag] INT            NOT NULL
);


GO
GRANT UPDATE
    ON OBJECT::[dbo].[CM_RPT_SecondVoice] TO [CORP\dmukherji]
    AS [dbo];


GO
GRANT SELECT
    ON OBJECT::[dbo].[CM_RPT_SecondVoice] TO [CORP\dmukherji]
    AS [dbo];


GO
GRANT DELETE
    ON OBJECT::[dbo].[CM_RPT_SecondVoice] TO [CORP\dmukherji]
    AS [dbo];


GO
GRANT SELECT
    ON OBJECT::[dbo].[CM_RPT_SecondVoice] TO [CORP\tkumar]
    AS [dbo];


GO
GRANT SELECT
    ON OBJECT::[dbo].[CM_RPT_SecondVoice] TO [corp\ravijaykumar]
    AS [dbo];


GO
GRANT SELECT
    ON OBJECT::[dbo].[CM_RPT_SecondVoice] TO [CORP\rsingh]
    AS [dbo];


GO
GRANT SELECT
    ON OBJECT::[dbo].[CM_RPT_SecondVoice] TO [CORP\musalunke]
    AS [dbo];


GO
GRANT SELECT
    ON OBJECT::[dbo].[CM_RPT_SecondVoice] TO [CORP\pjain]
    AS [dbo];

