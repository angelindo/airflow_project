from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 # Example:
                 # conn_id = your-connection-name
                 redshift_conn_id = '',
                 qas_checks = [],
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        # self.conn_id = conn_id
        self.redshift_conn_id = redshift_conn_id
        self.qas_checks = qas_checks

    def execute(self, context):
        self.log.info('DataQualityOperator beginning ...')
        
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
  
        for qas in self.qas_checks:
            query = qas['check_sql']
            exp_result = qas['expected_result']
            table = qas['table']
            
            records = redshift.get_records(query)
            
            if len(records) < 1 or len(records[0]) < 1:
                raise ValueError(f"Data quality check failed. {table} returned no results")
            
            num_records = records[0][0]
            
            if num_records>0:
                logging.error(f"At least one record is found in {table}")
                raise ValueError(f"At least one record is found in {table}")
        
            self.log.info.info(f"Data quality on table {table} check passed with {records[0][0]} records")
        
        self.log.info('All checks were OK ...')