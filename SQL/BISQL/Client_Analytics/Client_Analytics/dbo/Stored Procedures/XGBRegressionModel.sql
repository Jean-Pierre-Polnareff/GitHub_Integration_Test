

CREATE    PROCEDURE [XGBRegressionModel] (@trained_model varbinary(max) OUTPUT)
AS 
BEGIN
  DECLARE @inquery nvarchar(max) = N'
  SELECT  * FROM Analytics_2017.dbo.TrainData

'
  EXEC sp_execute_external_script @language = N'R',
                                  @script = N'
## Create model
		library(xgboost)
        library(dplyr)
        library(Matrix)
		   
		ModelDataP=data.frame(select(ModelData,-PaidOnAccountAmt))

		Data_matrix=xgb.DMatrix(data = as.matrix(ModelDataP), label = ModelData$PaidOnAccountAmt)

		h_model=xgb.train(data=Data_matrix,
                  nrounds=200,
                  objective = "reg:linear")
 ## Serialize model 

		trained_model <- as.raw(serialize(h_model, connection=NULL));  
'
 ,@input_data_1 = @inquery
 , @input_data_1_name = N'ModelData'
 , @params = N'@trained_model varbinary(max) OUTPUT'
 ,@trained_model = @trained_model OUTPUT; 
END;