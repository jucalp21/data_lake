const { S3Client, GetObjectCommand } = require("@aws-sdk/client-s3");
const { Readable } = require("stream");

const s3 = new S3Client({ region: "us-east-1" });

async function streamToString(stream) {
  return new Promise((resolve, reject) => {
    const chunks = [];
    stream.on("data", (chunk) => chunks.push(chunk));
    stream.on("error", reject);
    stream.on("end", () => resolve(Buffer.concat(chunks).toString("utf8")));
  });
}

exports.handler = async (event) => {
  const bucketName = "data-lake-demo";
  const key = `athena-results/164b6233-70cc-4da9-ad9f-0e42a9eeb1fc.csv`;

  const params = {
    Bucket: bucketName,
    Key: key,
  };

  try {
    const command = new GetObjectCommand(params);
    const data = await s3.send(command);
    const bodyContents = await streamToString(data.Body);

    return {
      statusCode: 200,
      body: bodyContents,
      headers: {
        "Content-Type": "text/csv",
      },
    };
  } catch (err) {
    return {
      statusCode: 500,
      body: JSON.stringify(err),
    };
  }
};
