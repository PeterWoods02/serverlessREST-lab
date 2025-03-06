import { APIGatewayProxyHandlerV2 } from "aws-lambda";
import { DynamoDBClient } from "@aws-sdk/client-dynamodb";
import { DynamoDBDocumentClient, DeleteCommand, GetCommand, QueryCommand } from "@aws-sdk/lib-dynamodb";

const ddbDocClient = createDDbDocClient();

export const handler: APIGatewayProxyHandlerV2 = async (event, context) => {
  try {
    console.log("[EVENT]", JSON.stringify(event));

    const pathParameters = event?.pathParameters;
    const movieId = pathParameters?.movieId ? parseInt(pathParameters.movieId) : undefined;
    const includeCast = event?.queryStringParameters?.cast === "true";
    
    if (!movieId) {
      return {
        statusCode: 400,
        headers: {
          "content-type": "application/json",
        },
        body: JSON.stringify({ message: "Missing or invalid movie ID" }),
      };
    }

    const getMovieCommand = new GetCommand({
      TableName: process.env.TABLE_NAME,
      Key: { id: movieId },
    });

    const movieResult = await ddbDocClient.send(getMovieCommand);

    if (!movieResult.Item) {
      return {
        statusCode: 404,
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ message: `Movie with ID ${movieId} not found.` }),
      };
    }

    let cast: Record<string, any>[] | null = null;
    if (includeCast) {
      const castTableName = process.env.CAST_TABLE_NAME; 
      if (castTableName) {
        const queryCommand = new QueryCommand({
          TableName: castTableName,
          KeyConditionExpression: "movieId = :movieId",
          ExpressionAttributeValues: {
            ":movieId": movieId,
          },
        });

        const castResult = await ddbDocClient.send(queryCommand);
        cast = castResult.Items || []; 
      } else {
        console.warn("CAST_TABLE_NAME environment variable is not set.");
      }
    }

    const response = {
      id: movieResult.Item.id,
      title: movieResult.Item.title,
      overview: movieResult.Item.overview,
      release_date: movieResult.Item.release_date,
      genre_ids: movieResult.Item.genre_ids,
      vote_average: movieResult.Item.vote_average,
      vote_count: movieResult.Item.vote_count,
      cast: cast || null, 
    };

    return {
      statusCode: 200,
      headers: {
        "content-type": "application/json",
      },
      body: JSON.stringify(response),
    };
  } catch (error: any) {
    console.log(JSON.stringify(error));
    return {
      statusCode: 500,
      headers: {
        "content-type": "application/json",
      },
      body: JSON.stringify({
        message: "Failed to fetch movie",
        error: error.message,
      }),
    };
  }
};

function createDDbDocClient() {
  const ddbClient = new DynamoDBClient({ region: process.env.REGION });

  const marshallOptions = {
    convertEmptyValues: true,
    removeUndefinedValues: true,
    convertClassInstanceToMap: true,
  };

  const unmarshallOptions = {
    wrapNumbers: false,
  };

  const translateConfig = { marshallOptions, unmarshallOptions };

  return DynamoDBDocumentClient.from(ddbClient, translateConfig);
}
