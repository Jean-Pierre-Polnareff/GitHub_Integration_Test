CREATE TABLE [dbo].[CM_Rpt_USAACMP] (
    [EMonth]            DATE           NULL,
    [Call_Date]         DATE           NULL,
    [Session_Id]        VARCHAR (100)  NULL,
    [Agent_Logon_Id]    VARCHAR (100)  NULL,
    [Transaction_Type]  VARCHAR (100)  NULL,
    [Livevox_Result]    VARCHAR (100)  NULL,
    [LV_Client_Name]    VARCHAR (100)  NULL,
    [Account_Number]    VARCHAR (100)  NULL,
    [Call_Duration]     INT            NULL,
    [Is_RPC]            INT            NOT NULL,
    [Is_PTP]            INT            NOT NULL,
    [CallCount]         INT            NOT NULL,
    [Is_Connect]        INT            NOT NULL,
    [DISSATOPP]         INT            NOT NULL,
    [SAMINEAPP]         INT            NOT NULL,
    [SAMINECOMP]        INT            NOT NULL,
    [EARLYHANGUP]       INT            NOT NULL,
    [VEROPP]            INT            NOT NULL,
    [CALLID]            BIGINT         NULL,
    [MRDHIT]            INT            NULL,
    [MRDTIME]           INT            NULL,
    [MMHIT]             INT            NULL,
    [MMTIME]            INT            NULL,
    [CRDHIT]            INT            NULL,
    [CRDTTIME]          INT            NULL,
    [BALHIT]            INT            NULL,
    [BALTIME]           INT            NULL,
    [SNDVCATHIT]        NVARCHAR (255) NULL,
    [SNDVCOMPNAME]      NVARCHAR (255) NULL,
    [SNDVSTTIME]        INT            NULL,
    [SNDVFLAG]          INT            NOT NULL,
    [CXLANGCOMPNAME]    NVARCHAR (255) NULL,
    [CXLANGCATG]        NVARCHAR (150) NULL,
    [CXLANGCATHIT]      NVARCHAR (255) NULL,
    [CXLANTIME]         INT            NULL,
    [NEGOSTARTCOMPNAME] NVARCHAR (255) NULL,
    [NEGTIMESTART]      INT            NULL,
    [NEGOFLAG]          INT            NULL,
    [NEGOENDCOMPNAME]   NVARCHAR (255) NULL,
    [NEGTIMEEND]        INT            NULL,
    [AMEFFORTCOMPNAME]  NVARCHAR (255) NULL,
    [AMEFFTTIME]        INT            NULL,
    [AMEFFORTFLAG]      INT            NULL,
    [OPNQUEST]          INT            NULL,
    [CLOQUEST]          INT            NULL,
    [VERFLAG]           INT            NOT NULL,
    [VERTIME]           INT            NULL,
    [H_Count]           INT            NULL,
    [M_Count]           INT            NULL,
    [L_Count]           INT            NULL,
    [DISSATSCR]         NUMERIC (3, 2) NULL,
    [TTNEG]             INT            NULL,
    [TTINPROB]          INT            NULL,
    [DIS1CAT]           NVARCHAR (10)  NULL,
    [TTOPENG]           INT            NULL,
    [MRDOPP]            INT            NOT NULL,
    [MMOPP]             INT            NOT NULL,
    [CRDOPP]            INT            NOT NULL,
    [BALOPP]            INT            NOT NULL,
    [NEGOPP]            INT            NOT NULL,
    [DISCOMPNAME]       NVARCHAR (255) NULL,
    [DISCOPTIME]        INT            NULL
);


GO
GRANT SELECT
    ON OBJECT::[dbo].[CM_Rpt_USAACMP] TO [CORP\musalunke]
    AS [dbo];


GO
GRANT SELECT
    ON OBJECT::[dbo].[CM_Rpt_USAACMP] TO [CORP\pjain]
    AS [dbo];

