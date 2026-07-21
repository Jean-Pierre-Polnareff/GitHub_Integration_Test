
CREATE   PROCEDURE [dbo].usp_model_score_thirdprod_expense (@model VARCHAR(100),@q NVARCHAR(MAX))
as 
DECLARE @rx_model VARBINARY(MAX) = (SELECT model FROM CLIENT_ANALYTICS.dbo.xgb_models WHERE model_name = @model);

    EXECUTE sp_execute_external_script
        @language = N'R'
      , @script = N'
		    library(xgboost)
            library(dplyr)
            library(Matrix)

		    test=InputDataSet;
						x_test=data.frame(select(test,c(age_acct_at_listdate_r,
														age_acct_at_chargeoff_r,
														has_previous_acct_thirdprod, 
														has_previous_acct_medprod,
														has_prev_rpc_rgs,
														state_level
											)));

		    model = unserialize(rx_model);	
		    pred=predict(model,as.matrix(x_test))
		    pred <- data.frame(pred);
		    score <- data.frame(pred,test$KeyCustomer);'

		 ,@input_data_1 =  @q
		 ,@output_data_1_name = N'score'
		 ,@params = N'@rx_model varbinary(max)'
         ,@rx_model = @rx_model
 WITH RESULT SETS (("score" FLOAT,"KeyCustomer" FLOAT))