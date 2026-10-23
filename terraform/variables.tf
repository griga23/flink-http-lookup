variable "confluent_cloud_api_key" {
  type      = string
  sensitive = true
}

variable "confluent_cloud_api_secret" {
  type      = string
  sensitive = true
}

variable "owner_user_id" {
  description = "Confluent user ID (u-...) that owns the resource-scoped API keys"
  type        = string
}

variable "environment_id" {
  type = string
}

variable "kafka_cluster_id" {
  type = string
}

variable "compute_pool_id" {
  type = string
}

variable "mock_api_base_url" {
  type = string
}

variable "deploy_connector" {
  description = "Create the HTTP sink connector (needs the part_requests topic to exist, i.e. after the Flink DDL has run)."
  type        = bool
  default     = false
}
