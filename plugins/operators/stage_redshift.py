from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.aws_hook import AwsHook


class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'

    copy_template = """
        COPY {destination_table}\
        FROM '{origin_path}'\
        ACCESS_KEY_ID '{access_key}'\
        SECRET_ACCESS_KEY '{secret_key}'\
        REGION AS '{region}'\
        FORMAT as json '{json_format}';
    """
        
    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 # Example:
                 redshift_conn_id = "",
                 destination_table = "",
                 origin_path = "",
                 aws_credentials_id = "",
                 region = "",
                 json_format = "",
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        self.redshift_conn_id = redshift_conn_id
        self.destination_table = destination_table
        self.origin_path = origin_path
        self.aws_credentials_id = aws_credentials_id
        self.region = region
        self.json_format = json_format
        

    def execute(self, context):
        
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        
        self.log.info('Deleting stage tables ... ')
        redshift.run("DELETE FROM {}".format(self.destination_table))
        
        self.log.info('Inserting stage tables ... ')
        copy_sql = StageToRedshiftOperator.copy_template.format(
            destination_table=self.destination_table,
            origin_path=self.origin_path,
            access_key=credentials.access_key,
            secret_key=credentials.secret_key,
            region=self.region,
            json_format=self.json_format
        )
        redshift.run(copy_sql)
#         self.log.info('StageToRedshiftOperator not implemented yet')






