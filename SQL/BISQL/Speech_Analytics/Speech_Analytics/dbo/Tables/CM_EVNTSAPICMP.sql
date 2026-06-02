CREATE TABLE [dbo].[CM_EVNTSAPICMP] (
    [EOM]      DATE           NULL,
    [CALLDT]   DATE           NULL,
    [CALLID]   BIGINT         NULL,
    [CATHIT]   NVARCHAR (255) NULL,
    [COMPNAME] NVARCHAR (255) NULL,
    [STTIME]   INT            NULL,
    [ENDTIME]  BIGINT         NULL
);








GO
CREATE NONCLUSTERED INDEX [IX_CM_EVNTSAPICMP_CALLID]
    ON [dbo].[CM_EVNTSAPICMP]([CALLID] ASC);




GO
GRANT UPDATE
    ON OBJECT::[dbo].[CM_EVNTSAPICMP] TO [CORP\dmukherji]
    AS [dbo];


GO
GRANT SELECT
    ON OBJECT::[dbo].[CM_EVNTSAPICMP] TO [CORP\dmukherji]
    AS [dbo];


GO
GRANT DELETE
    ON OBJECT::[dbo].[CM_EVNTSAPICMP] TO [CORP\dmukherji]
    AS [dbo];

