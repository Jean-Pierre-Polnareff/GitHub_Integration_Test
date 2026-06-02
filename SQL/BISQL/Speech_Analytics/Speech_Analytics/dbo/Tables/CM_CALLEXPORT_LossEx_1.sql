CREATE TABLE [dbo].[CM_CALLEXPORT_LossEx] (
    [CALLID]  BIGINT         NULL,
    [CALLDT]  DATE           NULL,
    [RGSID]   NVARCHAR (100) NULL,
    [RECDP]   NVARCHAR (15)  NULL,
    [RGSACC]  NVARCHAR (100) NULL,
    [CONFAV]  BIGINT         NULL,
    [DIS1]    NVARCHAR (100) NULL,
    [SKNM]    NVARCHAR (100) NULL,
    [SILT]    BIGINT         NULL,
    [CSST]    NVARCHAR (5)   NULL,
    [CLDUR]   BIGINT         NULL,
    [WRCNT]   BIGINT         NULL,
    [LSIL]    BIGINT         NULL,
    [AGSCR]   BIGINT         NULL,
    [DIR]     NVARCHAR (50)  NULL,
    [EOM]     DATE           NULL,
    [RGSSEID] NVARCHAR (100) NULL,
    [PHNUMB]  NVARCHAR (15)  NULL,
    [CLNTID]  NVARCHAR (200) NULL,
    [CATCNT]  BIGINT         NULL,
    [TRANFLG] INT            NULL,
    [EVNTFLG] INT            NULL
);


GO
GRANT UPDATE
    ON OBJECT::[dbo].[CM_CALLEXPORT_LossEx] TO [CORP\dmukherji]
    AS [dbo];


GO
GRANT SELECT
    ON OBJECT::[dbo].[CM_CALLEXPORT_LossEx] TO [CORP\dmukherji]
    AS [dbo];


GO
GRANT DELETE
    ON OBJECT::[dbo].[CM_CALLEXPORT_LossEx] TO [CORP\dmukherji]
    AS [dbo];


GO
GRANT SELECT
    ON OBJECT::[dbo].[CM_CALLEXPORT_LossEx] TO [CORP\pjain]
    AS [dbo];

