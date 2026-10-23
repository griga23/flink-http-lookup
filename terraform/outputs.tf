output "kafka_bootstrap" {
  value = data.confluent_kafka_cluster.this.bootstrap_endpoint
}

output "schema_registry_url" {
  value = data.confluent_schema_registry_cluster.this.rest_endpoint
}

output "flink_rest_endpoint" {
  value = data.confluent_flink_region.this.rest_endpoint
}

output "flink_organization_id" {
  value = data.confluent_organization.this.id
}

# terraform output -raw secrets_env > ../secrets.env
output "secrets_env" {
  sensitive = true
  value     = <<-EOT
    KAFKA_BOOTSTRAP=${data.confluent_kafka_cluster.this.bootstrap_endpoint}
    KAFKA_API_KEY=${confluent_api_key.kafka.id}
    KAFKA_API_SECRET=${confluent_api_key.kafka.secret}
    SR_URL=${data.confluent_schema_registry_cluster.this.rest_endpoint}
    SR_API_KEY=${confluent_api_key.schema_registry.id}
    SR_API_SECRET=${confluent_api_key.schema_registry.secret}
    FLINK_REST=${data.confluent_flink_region.this.rest_endpoint}
    FLINK_API_KEY=${confluent_api_key.flink.id}
    FLINK_API_SECRET=${confluent_api_key.flink.secret}
    FLINK_ORG_ID=${data.confluent_organization.this.id}
    FLINK_ENV_ID=${var.environment_id}
    FLINK_POOL_ID=${var.compute_pool_id}
    FLINK_CATALOG=${data.confluent_environment.this.display_name}
    FLINK_DATABASE=${data.confluent_kafka_cluster.this.display_name}
  EOT
}
