CREATE TABLE [dbo].[CM_EVNTSDT] (
    [EOM]     DATE   NULL,
    [CALLDT]  DATE   NULL,
    [CALLID]  BIGINT NULL,
    [SMPLFLG] INT    NULL
);








GO
CREATE NONCLUSTERED INDEX [IX_CM_EVNTSDT_CALLID]
    ON [dbo].[CM_EVNTSDT]([CALLID] ASC);




GO
GRANT UPDATE
    ON OBJECT::[dbo].[CM_EVNTSDT] TO [CORP\dmukherji]
    AS [dbo];


GO
GRANT SELECT
    ON OBJECT::[dbo].[CM_EVNTSDT] TO [CORP\dmukherji]
    AS [dbo];


GO
GRANT DELETE
    ON OBJECT::[dbo].[CM_EVNTSDT] TO [CORP\dmukherji]
    AS [dbo];

