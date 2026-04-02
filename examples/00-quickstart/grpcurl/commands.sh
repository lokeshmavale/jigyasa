// Health check
grpcurl -plaintext localhost:50051 jigyasa_dp_search.JigyasaDataPlaneService/Health

// Create collection
grpcurl -plaintext -d '{
  "collection": "quickstart",
  "indexSchema": "{\"fields\": [{\"name\": \"id\", \"type\": \"STRING\", \"key\": true, \"filterable\": true}, {\"name\": \"title\", \"type\": \"STRING\", \"searchable\": true}, {\"name\": \"body\", \"type\": \"STRING\", \"searchable\": true}]}"
}' localhost:50051 jigyasa_dp_search.JigyasaDataPlaneService/CreateCollection

// Index a document
grpcurl -plaintext -d '{
  "collection": "quickstart",
  "item": [{"document": "{\"id\": \"1\", \"title\": \"Hello Jigyasa\", \"body\": \"First document indexed\"}"}]
}' localhost:50051 jigyasa_dp_search.JigyasaDataPlaneService/Index

// Text search
grpcurl -plaintext -d '{
  "collection": "quickstart",
  "text_query": "hello",
  "include_source": true,
  "top_k": 5
}' localhost:50051 jigyasa_dp_search.JigyasaDataPlaneService/Query

// Count
grpcurl -plaintext -d '{"collection": "quickstart"}' localhost:50051 jigyasa_dp_search.JigyasaDataPlaneService/Count

// List collections
grpcurl -plaintext localhost:50051 jigyasa_dp_search.JigyasaDataPlaneService/ListCollections
