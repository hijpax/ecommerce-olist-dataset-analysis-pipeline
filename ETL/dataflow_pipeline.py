import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io import ReadFromText
from apache_beam.io.gcp.bigquery import WriteToBigQuery

# Define los nombres de los archivos y tablas
customers_csv = 'gs://nombre-del-bucket/customers.csv'
orders_csv = 'gs://nombre-del-bucket/orders.csv'
order_items_csv = 'gs://nombre-del-bucket/order_items.csv'
project_id = 'tu-proyecto-gcp'
dataset_id = 'tu-dataset'

# Define las opciones de la pipeline
pipeline_options = PipelineOptions()
pipeline_options.view_as(beam.options.pipeline_options.StandardOptions).runner = 'DataflowRunner'
pipeline_options.view_as(beam.options.pipeline_options.StandardOptions).project = project_id
pipeline_options.view_as(beam.options.pipeline_options.StandardOptions).region = 'us-central1'
pipeline_options.view_as(beam.options.pipeline_options.StandardOptions).temp_location = 'gs://nombre-del-bucket/temp'

# Define la función de transformación para procesar los datos de customers.csv
def process_customers(element):
    customer_id, customer_unique_id, customer_zip_code_prefix, customer_city, customer_state = element.split(',')
    # Realiza cualquier procesamiento adicional necesario
    return {
        'customer_id': customer_id,
        'customer_unique_id': customer_unique_id,
        'customer_zip_code_prefix': int(customer_zip_code_prefix),
        'customer_city': customer_city,
        'customer_state': customer_state
    }

# Define la función de transformación para procesar los datos de orders.csv
def process_orders(element):
    order_id, customer_id, order_status, purchase_timestamp, approved_at, carrier_date, customer_date, delivery_date, estimated_delivery_date = element.split(',')
    # Realiza cualquier procesamiento adicional necesario
    return {
        'order_id': order_id,
        'customer_id': customer_id,
        'order_status': order_status,
        'order_purchase_timestamp': purchase_timestamp,
        'order_approved_at': approved_at,
        'order_delivered_carrier_date': carrier_date,
        'order_delivered_customer_date': customer_date,
        'order_estimated_delivery_date': estimated_delivery_date
    }

# Define la función de transformación para procesar los datos de order_items.csv
def process_order_items(element):
    order_id, order_item_id, product_id, seller_id, shipping_limit_date, price, freight_value = element.split(',')
    # Realiza cualquier procesamiento adicional necesario
    return {
        'order_id': order_id,
        'order_item_id': order_item_id,
        'product_id': product_id,
        'seller_id': seller_id,
        'shipping_limit_date': shipping_limit_date,
        'price': float(price),
        'freight_value': float(freight_value)
    }

class ExecuteSQL(beam.DoFn):
    def __init__(self, project_id):
        self.project_id = project_id

    def process(self, element):
        script_path = element['script_path']

        # Lee el contenido del archivo de script SQL
        with open(script_path, 'r') as f:
            sql_script = f.read()

        # Ejecuta el script SQL utilizando la biblioteca google.cloud.bigquery
        client = bigquery.Client(project=self.project_id)
        job = client.query(sql_script)
        job.result()  # Espera a que la consulta se complete


# Crea la pipeline
with beam.Pipeline(options=pipeline_options) as pipeline:
    # Lee el archivo customers.csv y aplica la transformación
    customers_data = (
        pipeline
        | 'Read Customers CSV' >> ReadFromText(customers_csv)
        | 'Process Customers Data' >> beam.Map(process_customers)
    )

    # Lee el archivo orders.csv y aplica la transformación
    orders_data = (
        pipeline
        | 'Read Orders CSV' >> ReadFromText(orders_csv)
        | 'Process Orders Data' >> beam.Map(process_orders)
    )

    # Lee el archivo order_items.csv y aplica la transformación
    order_items_data = (
        pipeline
        | 'Read Order Items CSV' >> ReadFromText(order_items_csv)
        | 'Process Order Items Data' >> beam.Map(process_order_items)
    )

    # Escribe los datos en las tablas de BigQuery
    customers_data | 'Write Customers Data' >> WriteToBigQuery(
        table=f'{project_id}:{dataset_id}.customers',
        create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
        write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE
    )

    orders_data | 'Write Orders Data' >> WriteToBigQuery(
        table=f'{project_id}:{dataset_id}.orders',
        create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
        write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE
    )

    order_items_data | 'Write Order Items Data' >> WriteToBigQuery(
        table=f'{project_id}:{dataset_id}.order_items',
        create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
        write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE
    )

    # Ejecucion de la carga de datos en el modelo copo de nieve
    # Lectura del file que contiene los script sql
    script_path = 'gs://bucket/script.sql'  # Ruta al archivo SQL en el bucket de Google Cloud Storage

    # Crea un diccionario con la información del script a ejecutar
    script_info = {'script_path': script_path}

    # Ejecuta el script SQL en BigQuery
    pipeline | 'Create PCollection' >> beam.Create([script_info]) \
             | 'Execute SQL' >> beam.ParDo(ExecuteSQL('your-project-id'))