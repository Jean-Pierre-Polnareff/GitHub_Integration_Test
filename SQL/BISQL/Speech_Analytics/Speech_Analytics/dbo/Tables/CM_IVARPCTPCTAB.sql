CREATE TABLE [dbo].[CM_IVARPCTPCTAB] (
    [TITLE]     VARCHAR (9)    NOT NULL,
    [AGNTID]    NVARCHAR (100) NULL,
    [CALLID]    FLOAT (53)     NULL,
    [RGSACC]    NVARCHAR (100) NULL,
    [RECDP]     NVARCHAR (15)  NULL,
    [DIS1]      NVARCHAR (100) NULL,
    [SKNM]      NVARCHAR (100) NULL,
    [SILT]      FLOAT (53)     NULL,
    [CLDUR]     FLOAT (53)     NULL,
    [PHNUMB]    NVARCHAR (15)  NULL,
    [SLF_FLG]   INT            NULL,
    [MRD_FLG]   INT            NULL,
    [OPN_FLG]   INT            NULL,
    [CRD_FLG]   INT            NULL,
    [BAL_FLG]   INT            NULL,
    [XFR_FLG]   INT            NULL,
    [CXM_FLG]   INT            NULL,
    [SLF_OPP]   INT            NOT NULL,
    [MRD_OPP]   INT            NOT NULL,
    [OPN_OPP]   INT            NOT NULL,
    [CRD_OPP]   INT            NOT NULL,
    [BAL_OPP]   INT            NOT NULL,
    [XFR_OPP]   INT            NOT NULL,
    [ALTN_FLG]  INT            NULL,
    [SPS_FLG]   INT            NULL,
    [SPSQ_FLG]  INT            NULL,
    [SPSQ_FLG1] INT            NULL,
    [SPSV_FLG]  INT            NULL,
    [ALTQ_FLG]  INT            NULL,
    [CLLCNT]    INT            NOT NULL,
    [CXTYPE]    VARCHAR (11)   NOT NULL,
    [EOM]       DATE           NULL,
    [CALLDT]    DATE           NULL
);




GO
CREATE NONCLUSTERED INDEX [IX_CM_IVARPCTPCTAB_CALLID]
    ON [dbo].[CM_IVARPCTPCTAB]([CALLID] ASC) WITH (DATA_COMPRESSION = PAGE);


GO
GRANT UPDATE
    ON OBJECT::[dbo].[CM_IVARPCTPCTAB] TO [CORP\tkumar]
    AS [dbo];


GO
GRANT UPDATE
    ON OBJECT::[dbo].[CM_IVARPCTPCTAB] TO [CORP\pjain]
    AS [dbo];


GO
GRANT UPDATE
    ON OBJECT::[dbo].[CM_IVARPCTPCTAB] TO [CORP\musalunke]
    AS [dbo];


GO
GRANT UPDATE
    ON OBJECT::[dbo].[CM_IVARPCTPCTAB] TO [CORP\mhuang]
    AS [dbo];


GO
GRANT UPDATE
    ON OBJECT::[dbo].[CM_IVARPCTPCTAB] TO [CORP\dmukherji]
    AS [dbo];


GO
GRANT UPDATE
    ON OBJECT::[dbo].[CM_IVARPCTPCTAB] TO [CORP\aughodake]
    AS [dbo];


GO
GRANT UPDATE
    ON OBJECT::[dbo].[CM_IVARPCTPCTAB] TO [CORP\aramugade]
    AS [dbo];


GO
GRANT SELECT
    ON OBJECT::[dbo].[CM_IVARPCTPCTAB] TO [CORP\tkumar]
    AS [dbo];


GO
GRANT SELECT
    ON OBJECT::[dbo].[CM_IVARPCTPCTAB] TO [CORP\pjain]
    AS [dbo];


GO
GRANT SELECT
    ON OBJECT::[dbo].[CM_IVARPCTPCTAB] TO [CORP\musalunke]
    AS [dbo];


GO
GRANT SELECT
    ON OBJECT::[dbo].[CM_IVARPCTPCTAB] TO [CORP\mhuang]
    AS [dbo];


GO
GRANT SELECT
    ON OBJECT::[dbo].[CM_IVARPCTPCTAB] TO [CORP\dmukherji]
    AS [dbo];


GO
GRANT SELECT
    ON OBJECT::[dbo].[CM_IVARPCTPCTAB] TO [CORP\aughodake]
    AS [dbo];


GO
GRANT SELECT
    ON OBJECT::[dbo].[CM_IVARPCTPCTAB] TO [CORP\aramugade]
    AS [dbo];


GO
GRANT DELETE
    ON OBJECT::[dbo].[CM_IVARPCTPCTAB] TO [CORP\tkumar]
    AS [dbo];


GO
GRANT DELETE
    ON OBJECT::[dbo].[CM_IVARPCTPCTAB] TO [CORP\pjain]
    AS [dbo];


GO
GRANT DELETE
    ON OBJECT::[dbo].[CM_IVARPCTPCTAB] TO [CORP\musalunke]
    AS [dbo];


GO
GRANT DELETE
    ON OBJECT::[dbo].[CM_IVARPCTPCTAB] TO [CORP\mhuang]
    AS [dbo];


GO
GRANT DELETE
    ON OBJECT::[dbo].[CM_IVARPCTPCTAB] TO [CORP\dmukherji]
    AS [dbo];


GO
GRANT DELETE
    ON OBJECT::[dbo].[CM_IVARPCTPCTAB] TO [CORP\aughodake]
    AS [dbo];


GO
GRANT DELETE
    ON OBJECT::[dbo].[CM_IVARPCTPCTAB] TO [CORP\aramugade]
    AS [dbo];

