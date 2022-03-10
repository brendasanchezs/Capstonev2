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
                 redshift_conn_id="",
                 destination_table="",
                 mode="",
                 sql="",
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        # self.conn_id = conn_id
        self.redshift_conn_id = redshift_conn_id
        self.sql=sql
        self.mode=mode
        self.destination_table=destination_table

    def execute(self, context):
        #self.log.info('LoadDimensionOperator not implemented yet')
        redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        if self.mode=='truncate':
            self.log.info(f"Clearing data from {self.destination_table}")
            redshift_hook.run(f"DELETE FROM {self.destination_table}")
        redshift_hook.run(self.sql)