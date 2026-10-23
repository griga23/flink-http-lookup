data "confluent_environment" "this" {
  id = var.environment_id
}

data "confluent_kafka_cluster" "this" {
  id = var.kafka_cluster_id
  environment {
    id = var.environment_id
  }
}

data "confluent_schema_registry_cluster" "this" {
  environment {
    id = var.environment_id
  }
}

data "confluent_flink_compute_pool" "this" {
  id = var.compute_pool_id
  environment {
    id = var.environment_id
  }
}

data "confluent_flink_region" "this" {
  cloud  = data.confluent_flink_compute_pool.this.cloud
  region = data.confluent_flink_compute_pool.this.region
}

# ---------------------------------------------------------------- API keys (owned by the user)
locals {
  owner = {
    id          = var.owner_user_id
    api_version = "iam/v2"
    kind        = "User"
  }
}

resource "confluent_api_key" "kafka" {
  display_name = "flink-http-lookup-demo-kafka"
  owner {
    id          = local.owner.id
    api_version = local.owner.api_version
    kind        = local.owner.kind
  }
  managed_resource {
    id          = data.confluent_kafka_cluster.this.id
    api_version = data.confluent_kafka_cluster.this.api_version
    kind        = data.confluent_kafka_cluster.this.kind
    environment {
      id = var.environment_id
    }
  }
}

resource "confluent_api_key" "schema_registry" {
  display_name = "flink-http-lookup-demo-sr"
  owner {
    id          = local.owner.id
    api_version = local.owner.api_version
    kind        = local.owner.kind
  }
  managed_resource {
    id          = data.confluent_schema_registry_cluster.this.id
    api_version = data.confluent_schema_registry_cluster.this.api_version
    kind        = data.confluent_schema_registry_cluster.this.kind
    environment {
      id = var.environment_id
    }
  }
}

# ---------------------------------------------------------------- topics
# machine_events and part_requests are created by Flink DDL (sql/01-tables.sql).
# The connector's result/error topics are created here so Flink can read them.
resource "confluent_kafka_topic" "part_responses" {
  kafka_cluster {
    id = data.confluent_kafka_cluster.this.id
  }
  topic_name       = "part_responses"
  partitions_count = 3
  rest_endpoint    = data.confluent_kafka_cluster.this.rest_endpoint
  credentials {
    key    = confluent_api_key.kafka.id
    secret = confluent_api_key.kafka.secret
  }
}

resource "confluent_kafka_topic" "part_lookup_errors" {
  kafka_cluster {
    id = data.confluent_kafka_cluster.this.id
  }
  topic_name       = "part_lookup_errors"
  partitions_count = 3
  rest_endpoint    = data.confluent_kafka_cluster.this.rest_endpoint
  credentials {
    key    = confluent_api_key.kafka.id
    secret = confluent_api_key.kafka.secret
  }
}

resource "confluent_kafka_topic" "part_lookup_dlq" {
  kafka_cluster {
    id = data.confluent_kafka_cluster.this.id
  }
  topic_name       = "part_lookup_dlq"
  partitions_count = 1
  rest_endpoint    = data.confluent_kafka_cluster.this.rest_endpoint
  credentials {
    key    = confluent_api_key.kafka.id
    secret = confluent_api_key.kafka.secret
  }
}

resource "confluent_api_key" "flink" {
  display_name = "flink-http-lookup-demo-flink"
  owner {
    id          = local.owner.id
    api_version = local.owner.api_version
    kind        = local.owner.kind
  }
  managed_resource {
    id          = data.confluent_flink_region.this.id
    api_version = data.confluent_flink_region.this.api_version
    kind        = data.confluent_flink_region.this.kind
    environment {
      id = var.environment_id
    }
  }
}

data "confluent_organization" "this" {}
