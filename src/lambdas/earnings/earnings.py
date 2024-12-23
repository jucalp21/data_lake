import boto3
import json
import time
from datetime import datetime, timedelta


def lambda_handler(event, context):
    headers = {
        'Access-Control-Allow-Origin': '*',
        'Access-Control-Allow-Methods': 'POST, OPTIONS',
        'Access-Control-Allow-Headers': 'Content-Type, Authorization'
    }

    if event['httpMethod'] == 'OPTIONS':
        return {
            'statusCode': 200,
            'headers': headers,
            'body': json.dumps('Preflight OK')
        }

    try:
        body = json.loads(event['body'])
    except Exception:
        return {
            'statusCode': 400,
            'headers': headers,
            'body': json.dumps('Error al procesar el cuerpo de la solicitud.')
        }

    start_date = body.get('start_date')
    locations = body.get('locations')
    user_selected = body.get('userSelected')
    platform = body.get('platform')

    if not start_date:
        return {
            'statusCode': 400,
            'headers': headers,
            'body': json.dumps('Debe proporcionar start_date en el formato YYYY-MM-DD')
        }

    try:
        datetime.strptime(start_date, '%Y-%m-%d')
    except ValueError:
        return {
            'statusCode': 400,
            'headers': headers,
            'body': json.dumps('Formato de fecha inválido. Use YYYY-MM-DD.')
        }

    filters_main = []

    if locations:
        for loc in locations:
            if 'officeName' in loc and loc['officeName']:
                office_filter = loc['officeName'].replace("'", "''")
                filters_main.append(f"us.office = '{office_filter}'")
            elif 'cityName' in loc and loc['cityName']:
                city_filter = loc['cityName'].replace("'", "''")
                filters_main.append(f"us.city = '{city_filter}'")

    if user_selected:
        user_selected_filter = user_selected.replace("'", "''")
        filters_main.append(f"us._id = '{user_selected_filter}'")

    filters_main_str = f" AND ({' OR '.join(filters_main)})" if filters_main else ""

    # Dependiendo de la plataforma, armamos la consulta SQL
    if platform == "streamate":
        query = f"""
            SELECT  ssmp.date AS report_date,
                    'Streamate' AS source,
                    SUM(CAST(ssmp.total_earnings AS DOUBLE)) AS totalAmount
            FROM    "data_lake_pdn_og"."silver_streamate_model_performance" ssmp
            INNER JOIN "data_lake_pdn_og"."bronze_users" us ON ssmp._id = us._id
            WHERE   CAST(ssmp.date AS DATE) >= DATE('{start_date}')
            {filters_main_str}
            GROUP BY ssmp.date
            ORDER BY ssmp.date ASC
        """
    elif platform == "jasmin":
        query = f"""
            SELECT  jsmp.date AS report_date,
                    'Jasmin' AS source,
                    SUM(CAST(jsmp.total_earnings AS DOUBLE)) AS totalAmount
            FROM    "data_lake_pdn_og"."silver_jasmin_model_performance" jsmp
            INNER JOIN "data_lake_pdn_og"."bronze_users" us ON jsmp._id = us._id
            WHERE   CAST(jsmp.date AS DATE) >= DATE('{start_date}')
            {filters_main_str}
            GROUP BY jsmp.date
            ORDER BY jsmp.date ASC
        """
    else:  # Si no llega plataforma, hacemos UNION ALL de ambas fuentes
        query = f"""
            SELECT  ssmp.date AS report_date,
                    'Streamate' AS source,
                    SUM(CAST(ssmp.total_earnings AS DOUBLE)) AS totalAmount
            FROM    "data_lake_pdn_og"."silver_streamate_model_performance" ssmp
            INNER JOIN "data_lake_pdn_og"."bronze_users" us ON ssmp._id = us._id
            WHERE   CAST(ssmp.date AS DATE) >= DATE('{start_date}')
            {filters_main_str}
            GROUP BY ssmp.date
            
            UNION ALL
            SELECT  jsmp.date AS report_date,
                    'Jasmin' AS source,
                    SUM(CAST(jsmp.total_earnings AS DOUBLE)) AS totalAmount
            FROM    "data_lake_pdn_og"."silver_jasmin_model_performance" jsmp
            INNER JOIN "data_lake_pdn_og"."bronze_users" us ON jsmp._id = us._id
            WHERE   CAST(jsmp.date AS DATE) >= DATE('{start_date}')
            {filters_main_str}
            GROUP BY jsmp.date
            ORDER BY report_date ASC
        """

    # Athena query execution
    athena_client = boto3.client('athena')
    database = 'data_lake_pdn_og'
    output_location = 's3://data-lake-prd-og/gold/'

    try:
        response = athena_client.start_query_execution(
            QueryString=query,
            QueryExecutionContext={'Database': database},
            ResultConfiguration={'OutputLocation': output_location}
        )

        query_execution_id = response['QueryExecutionId']

        max_wait_time = 60
        waited_time = 0
        sleep_time = 2

        while waited_time < max_wait_time:
            query_status = athena_client.get_query_execution(
                QueryExecutionId=query_execution_id)
            query_state = query_status['QueryExecution']['Status']['State']

            if query_state == 'SUCCEEDED':
                break
            elif query_state in ['FAILED', 'CANCELLED']:
                return {
                    'statusCode': 500,
                    'headers': headers,
                    'body': json.dumps(f"Query {query_state} con motivo: {query_status['QueryExecution']['Status']['StateChangeReason']}")
                }

            time.sleep(sleep_time)
            waited_time += sleep_time

        if waited_time >= max_wait_time:
            return {
                'statusCode': 500,
                'headers': headers,
                'body': json.dumps('Timeout al esperar que la consulta se ejecute.')
            }

        result = athena_client.get_query_results(
            QueryExecutionId=query_execution_id)
        rows = result['ResultSet']['Rows']

        # Initialize the result structure
        result_data = {}

        # Convert Athena results to a dictionary by date
        result_dict = {
            "streamate": {},
            "jasmin": {}
        }

        for row in rows[1:]:
            date = row['Data'][0]['VarCharValue']
            platform = row['Data'][1]['VarCharValue']
            total_amount = float(row['Data'][2]['VarCharValue'])

            if platform == "Streamate":
                result_dict["streamate"][date] = total_amount
            elif platform == "Jasmin":
                result_dict["jasmin"][date] = total_amount

        # Find the last available date from the query results
        available_dates = sorted(set(result_dict["streamate"].keys()).union(
            set(result_dict["jasmin"].keys())))

        # Add Streamate data only if there is data for Streamate
        if result_dict["streamate"]:
            result_data["streamate"] = []
            for date_str in available_dates:
                if date_str in result_dict["streamate"]:
                    result_data["streamate"].append({
                        "date": date_str,
                        "totalAmount": result_dict["streamate"][date_str]
                    })
                else:
                    result_data["streamate"].append({
                        "date": date_str,
                        "totalAmount": None  # Or 0 if you prefer
                    })

        # Always include Jasmin if there is data
        if result_dict["jasmin"]:
            result_data["jasmin"] = []
            for date_str in available_dates:
                if date_str in result_dict["jasmin"]:
                    result_data["jasmin"].append({
                        "date": date_str,
                        "totalAmount": result_dict["jasmin"][date_str]
                    })
                else:
                    result_data["jasmin"].append({
                        "date": date_str,
                        "totalAmount": None  # Or 0 if you prefer
                    })

        # Return the response with the formatted data
        return {
            'statusCode': 200,
            'headers': headers,
            'body': json.dumps(result_data)
        }

    except Exception as e:
        return {
            'statusCode': 500,
            'headers': headers,
            'body': json.dumps(f"Error al ejecutar la consulta: {str(e)}")
        }
