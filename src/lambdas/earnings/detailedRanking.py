import boto3
import json
import time
import urllib3
from datetime import datetime


def fetch_api_data(api_url, api_authorization):
    http = urllib3.PoolManager()
    headers = {"Authorization": api_authorization}
    response = http.request("GET", api_url, headers=headers)
    if response.status != 200:
        raise Exception(f"Error al consultar el API: {response.status}")
    return json.loads(response.data.decode("utf-8"))


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
    end_date = body.get('end_date')
    locations = body.get('locations')
    api_authorization = body.get('authorization')
    page = int(body.get('page', 1))
    limit = int(body.get('limit', 10))

    if not start_date or not end_date or not api_authorization:
        return {
            'statusCode': 400,
            'headers': headers,
            'body': json.dumps('Debe proporcionar start_date, end_date y authorization en el cuerpo.')
        }

    try:
        datetime.strptime(start_date, '%Y-%m-%d')
        datetime.strptime(end_date, '%Y-%m-%d')
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

    filters_main_str = f" AND ({' OR '.join(filters_main)})" if filters_main else ""

    query = f"""
        WITH earnings_data AS (
            SELECT
                bu.artisticname,
                bu.jasminuser,
                bu.streamateuser,
                bu.city,
                bu.office,
                bu._id,
                CASE
                    WHEN eap.emailaddress = bu.jasminuser THEN 'jasmin'
                    WHEN eap.emailaddress = bu.streamateuser THEN 'streamate'
                END AS platform,
                SUM(eap.payableamount) AS sales,
                SUM(eap.onlineseconds) AS time
            FROM "data_lake_db"."silver_earnings_by_performer" eap
            INNER JOIN "data_lake_db"."bronze_users" bu
                ON eap.emailaddress = bu.jasminuser OR eap.emailaddress = bu.streamateuser
            WHERE CAST(eap."date" AS DATE) BETWEEN DATE('{start_date}') AND DATE('{end_date}')
            {filters_main_str}
            GROUP BY bu.artisticname, bu.jasminuser, bu.streamateuser, bu.city, bu.office, bu._id,
                    CASE
                        WHEN eap.emailaddress = bu.jasminuser THEN 'jasmin'
                        WHEN eap.emailaddress = bu.streamateuser THEN 'streamate'
                    END
        )
        SELECT
            artisticname,
            jasminuser AS user,
            city,
            office,
            _id,
            platform,
            sales,
            time,
            ROUND((sales / SUM(sales) OVER (PARTITION BY artisticname)) * 100, 2) AS percentage,
            SUM(sales) OVER (PARTITION BY artisticname) AS total
        FROM earnings_data
        ORDER BY artisticname, platform;
    """

    athena_client = boto3.client('athena')
    database = 'data_lake_db'
    output_location = 's3://data-lake-demo/gold/'

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

        if len(rows) <= 1:
            return {
                'statusCode': 200,
                'headers': headers,
                'body': json.dumps('No se encontraron resultados para el rango de fechas especificado.')
            }

        api_url = "https://1astats.omgworldwidegroup.com/api/v1/user"
        try:
            models_data = {model["_id"]: model for model in fetch_api_data(
                api_url, api_authorization).get("users", [])}
        except Exception as api_error:
            return {
                'statusCode': 500,
                'headers': headers,
                'body': json.dumps(f"Error al consultar el API: {str(api_error)}")
            }

        # Consolidar los datos por _id
        output_dict = {}
        for row in rows[1:]:
            artistic_name = row['Data'][0].get('VarCharValue', None)
            _id = row['Data'][4].get('VarCharValue', None)

            if not artistic_name or not _id:
                continue

            if _id not in output_dict:
                output_dict[_id] = {
                    "_id": _id,
                    "model": {
                        "artisticName": artistic_name,
                        "user": row['Data'][1].get('VarCharValue', None),
                        "role": "model",
                        "city": row['Data'][2].get('VarCharValue', None),
                        "office": row['Data'][3].get('VarCharValue', None),
                        "picture": models_data.get(_id, {}).get("picture", None)
                    },
                    "jasmin": {},
                    "streamate": {},
                    "total": 0
                }

            data = output_dict[_id]
            platform = row['Data'][5].get('VarCharValue', None)
            platform_data = {
                "sales": float(row['Data'][6].get('VarCharValue', 0)),
                "time": int(row['Data'][7].get('VarCharValue', 0)),
                "percentage": float(row['Data'][8].get('VarCharValue', 0))
            }
            if platform:
                data[platform] = platform_data
            data["total"] += platform_data["sales"]

        # Convertir a lista y paginar
        output = list(output_dict.values())
        start_index = (page - 1) * limit
        end_index = start_index + limit
        paginated_output = output[start_index:end_index]
        has_more = end_index < len(output)

        return {
            'statusCode': 200,
            'headers': headers,
            'body': json.dumps({
                "page": page,
                "limit": limit,
                "total_results": len(output),
                "hasMore": has_more,
                "results": paginated_output
            })
        }

    except Exception as e:
        return {
            'statusCode': 500,
            'headers': headers,
            'body': json.dumps(f"Error al ejecutar la consulta: {str(e)}")
        }