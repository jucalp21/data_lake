const AWS = require("aws-sdk");
const athena = new AWS.Athena();

exports.handler = async (event) => {
  const params = {
    QueryString: "SELECT * FROM your_table WHERE condition",
    ResultConfiguration: {
      OutputLocation: "s3://your-bucket/query-results/",
    },
  };

  try {
    const queryExecution = await athena.startQueryExecution(params).promise();
    const queryExecutionId = queryExecution.QueryExecutionId;

    // Espera a que la consulta se complete
    await waitForQueryToComplete(queryExecutionId);

    // Obtén los resultados de la consulta
    const results = await getQueryResults(queryExecutionId);

    return {
      statusCode: 200,
      body: JSON.stringify(results),
    };
  } catch (error) {
    return {
      statusCode: 500,
      body: JSON.stringify(error),
    };
  }
};

async function waitForQueryToComplete(queryExecutionId) {
  const params = {
    QueryExecutionId: queryExecutionId,
  };

  while (true) {
    const queryExecution = await athena.getQueryExecution(params).promise();
    const state = queryExecution.QueryExecution.Status.State;

    if (state === "SUCCEEDED") {
      break;
    } else if (state === "FAILED" || state === "CANCELLED") {
      throw new Error(`Query failed or was cancelled: ${state}`);
    }

    // Espera 5 segundos antes de verificar nuevamente
    await new Promise((resolve) => setTimeout(resolve, 5000));
  }
}

async function getQueryResults(queryExecutionId) {
  const params = {
    QueryExecutionId: queryExecutionId,
  };

  const results = await athena.getQueryResults(params).promise();
  return results.ResultSet.Rows;
}
