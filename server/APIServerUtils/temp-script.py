import sys
import os

from sqlalchemy.sql.expression import Null

# Add the project root to the path
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from server.APIServerUtils.db_validation_service import DatabaseValidationService

try:
    # Initialize the validation service
    validation_service = DatabaseValidationService()
    
    # Call the method with all required parameters
    result = validation_service.get_latest_process_instance_summary(
        client_id=2, 
        fund_id=1, 
        subproduct_id=1, 
        source_a='Bluefield',  # Empty string or None if not applicable
        # source_b='',  # Empty string or None if not applicable
        date_a='2024-01-31', 
        date_b='2024-02-29'
    )
    # result = validation_service.get_validation_aggregated_data(
    #     process_instance_id=39,
    #     client_id=2
    # )

    # result = validation_service.get_report_ingested_data(
    #     client_id=2,
    #     fund_id=1,
    #     # subproduct_id=1,
    #     source_a='Bluefield',
    #     date_a='2024-01-31', 
    #     date_b='2024-02-29'
    # )

    # df = validation_service.get_data_model_data_df(
    #     client_id=2,
    #     fund_id=1,
    #     period='2024-01-31',
    #     data_model_name='Portfolio Valuation By Instrument',
    #     source_name='Harborview'
    # )
    # print(df.head())


    print(result)

except Exception as e:
    import traceback
    print(f"Error: {e}")
    traceback.print_exc()
