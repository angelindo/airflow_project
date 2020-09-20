from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 # Example:
                 # conn_id = your-connection-name
                 redshift_conn_id = '',
                 destination_table = '',
                 select_query = '',
                 truncate_first = False,
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        # self.conn_id = conn_id
        self.redshift_conn_id = redshift_conn_id
        self.destination_table = destination_table
        self.select_query = select_query
        self.truncate_first = truncate_first

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        if self.truncate_first:
            self.log.info('Truncate dimension table {} ... '.format(self.destination_table))
            redshift.run('''TRUNCATE TABLE {} '''.format(self.destination_table))
 
        self.log.info('Inserting dimension table ... ')
        insert_query = '''INSERT INTO {} {} '''.format(self.destination_table, self.select_query)
        redshift.run(insert_query)
        