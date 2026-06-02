CREATE TABLE [dbo].[CM_BALCALLEXPORT_float_issue] (
    [AGNTID]       NVARCHAR (100) NULL,
    [CALLID]       FLOAT (53)     NULL,
    [CALLDT]       DATE           NULL,
    [RGSID]        NVARCHAR (100) NULL,
    [float CALLID] FLOAT (53)     NULL
);




GO
GRANT UPDATE
    ON OBJECT::[dbo].[CM_BALCALLEXPORT_float_issue] TO [CORP\dmukherji]
    AS [dbo];


GO
GRANT SELECT
    ON OBJECT::[dbo].[CM_BALCALLEXPORT_float_issue] TO [CORP\dmukherji]
    AS [dbo];


GO
GRANT DELETE
    ON OBJECT::[dbo].[CM_BALCALLEXPORT_float_issue] TO [CORP\dmukherji]
    AS [dbo];

