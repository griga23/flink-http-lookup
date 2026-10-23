resource "confluent_connector" "part_lookup" {
  count = var.deploy_connector ? 1 : 0

  environment {
    id = var.environment_id
  }
  kafka_cluster {
    id = var.kafka_cluster_id
  }

  config_sensitive = {
    "kafka.api.key"    = confluent_api_key.kafka.id
    "kafka.api.secret" = confluent_api_key.kafka.secret
  }

  config_nonsensitive = {
    "connector.class"              = "HttpSinkV2"
    "name"                         = "part-lookup-http-sink"
    "kafka.auth.mode"              = "KAFKA_API_KEY"
    "topics"                       = "part_requests"
    "input.data.format"            = "AVRO"
    "tasks.max"                    = "2"
    "http.api.base.url"            = var.mock_api_base_url
    "auth.type"                    = "NONE"
    "https.ssl.enabled"            = "true"
    "behavior.on.error"            = "IGNORE"
    "apis.num"                     = "1"
    "api1.topics"                  = "part_requests"
    "api1.http.request.method"     = "GET"
    "api1.http.api.path"           = "/api/v1/parts/$${part_number}"
    "api1.max.batch.size"          = "1"
    "api1.max.retries"             = "3"
    "api1.retry.on.status.codes"   = "500-"
    "api1.http.connect.timeout.ms" = "10000"
    "api1.http.request.timeout.ms" = "10000"

    "reporter.result.topic.name"        = confluent_kafka_topic.part_responses.topic_name
    "reporter.error.topic.name"         = confluent_kafka_topic.part_lookup_errors.topic_name
    "errors.deadletterqueue.topic.name" = confluent_kafka_topic.part_lookup_dlq.topic_name
    "report.errors.as"                  = "http_response"
  }
}
