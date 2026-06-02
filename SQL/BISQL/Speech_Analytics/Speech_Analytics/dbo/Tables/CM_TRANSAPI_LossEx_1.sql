CREATE TABLE [dbo].[CM_TRANSAPI_LossEx] (
    [EOM]    DATE           NULL,
    [CALLDT] DATE           NULL,
    [CALLID] FLOAT (53)     NULL,
    [WORD]   NVARCHAR (150) NULL,
    [SPKR]   NVARCHAR (50)  NULL,
    [STTIME] FLOAT (53)     NULL
);


GO
GRANT UPDATE
    ON OBJECT::[dbo].[CM_TRANSAPI_LossEx] TO [CORP\dmukherji]
    AS [dbo];


GO
GRANT SELECT
    ON OBJECT::[dbo].[CM_TRANSAPI_LossEx] TO [CORP\dmukherji]
    AS [dbo];


GO
GRANT DELETE
    ON OBJECT::[dbo].[CM_TRANSAPI_LossEx] TO [CORP\dmukherji]
    AS [dbo];


GO
GRANT SELECT
    ON OBJECT::[dbo].[CM_TRANSAPI_LossEx] TO [CORP\pjain]
    AS [dbo];

