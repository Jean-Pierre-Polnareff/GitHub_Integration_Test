CREATE TABLE [dbo].[CM_Rpt_AXPLegalPayInfo] (
    [EOM]            DATE            NULL,
    [AGNTID]         NVARCHAR (100)  NULL,
    [CALLID]         BIGINT          NULL,
    [CALLDT]         DATE            NULL,
    [RECDP]          NVARCHAR (100)  NULL,
    [RGSACC]         NVARCHAR (100)  NULL,
    [DIS1]           NVARCHAR (100)  NULL,
    [CLNTID]         NVARCHAR (200)  NULL,
    [SKNM]           NVARCHAR (100)  NULL,
    [CSST]           NVARCHAR (30)   NULL,
    [CLDUR]          INT             NULL,
    [DIR]            NVARCHAR (50)   NULL,
    [RGSSEID]        NVARCHAR (4000) NULL,
    [PHNUMB]         NVARCHAR (30)   NULL,
    [NewValue]       VARCHAR (2000)  NULL,
    [AMPYIFOSTTIM]   INT             NULL,
    [VERFLAG]        INT             NULL,
    [VERIFRLANGSTME] INT             NULL,
    [CXPAYINFOSTTIM] INT             NULL,
    [TMDIFF]         INT             NULL,
    [CALLCOUNT]      INT             NOT NULL,
    [POSSBLEINF]     INT             NOT NULL,
    [PNDFLAG]        INT             NOT NULL,
    [AMIFOFLAG]      INT             NOT NULL
);


GO
GRANT SELECT
    ON OBJECT::[dbo].[CM_Rpt_AXPLegalPayInfo] TO [CORP\musalunke]
    AS [dbo];


GO
GRANT SELECT
    ON OBJECT::[dbo].[CM_Rpt_AXPLegalPayInfo] TO [CORP\pjain]
    AS [dbo];

