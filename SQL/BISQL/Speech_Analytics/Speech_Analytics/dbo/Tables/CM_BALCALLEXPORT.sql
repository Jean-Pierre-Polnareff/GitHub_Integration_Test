CREATE TABLE [dbo].[CM_BALCALLEXPORT] (
    [AGNTID]  NVARCHAR (100) NULL,
    [CALLID]  BIGINT         NULL,
    [CALLDT]  DATE           NULL,
    [RGSID]   NVARCHAR (100) NULL,
    [RECDP]   NVARCHAR (15)  NULL,
    [RGSACC]  NVARCHAR (100) NULL,
    [CONFAV]  INT            NULL,
    [DIS1]    NVARCHAR (100) NULL,
    [SKNM]    NVARCHAR (100) NULL,
    [SILT]    INT            NULL,
    [CSST]    NVARCHAR (5)   NULL,
    [CLDUR]   INT            NULL,
    [WRCNT]   INT            NULL,
    [LSIL]    INT            NULL,
    [AGSCR]   INT            NULL,
    [DIR]     NVARCHAR (50)  NULL,
    [EOM]     DATE           NULL,
    [RGSSEID] NVARCHAR (100) NULL,
    [PHNUMB]  NVARCHAR (15)  NULL,
    [CLNTID]  NVARCHAR (200) NULL,
    [CATCNT]  INT            NULL
);






GO
GRANT UPDATE
    ON OBJECT::[dbo].[CM_BALCALLEXPORT] TO [CORP\dmukherji]
    AS [dbo];


GO
GRANT SELECT
    ON OBJECT::[dbo].[CM_BALCALLEXPORT] TO [CORP\dmukherji]
    AS [dbo];


GO
GRANT DELETE
    ON OBJECT::[dbo].[CM_BALCALLEXPORT] TO [CORP\dmukherji]
    AS [dbo];

