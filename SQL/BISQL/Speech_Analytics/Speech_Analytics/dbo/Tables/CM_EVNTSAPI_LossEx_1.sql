CREATE TABLE [dbo].[CM_EVNTSAPI_LossEx] (
    [EOM]      DATE           NULL,
    [CALLDT]   DATE           NULL,
    [CALLID]   FLOAT (53)     NULL,
    [CATHIT]   NVARCHAR (255) NULL,
    [COMPNAME] NVARCHAR (255) NULL,
    [STTIME]   FLOAT (53)     NULL,
    [ENDTIME]  FLOAT (53)     NULL
);


GO
GRANT UPDATE
    ON OBJECT::[dbo].[CM_EVNTSAPI_LossEx] TO [CORP\dmukherji]
    AS [dbo];


GO
GRANT SELECT
    ON OBJECT::[dbo].[CM_EVNTSAPI_LossEx] TO [CORP\dmukherji]
    AS [dbo];


GO
GRANT DELETE
    ON OBJECT::[dbo].[CM_EVNTSAPI_LossEx] TO [CORP\dmukherji]
    AS [dbo];


GO
GRANT SELECT
    ON OBJECT::[dbo].[CM_EVNTSAPI_LossEx] TO [CORP\pjain]
    AS [dbo];

