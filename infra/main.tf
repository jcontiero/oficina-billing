terraform {
  required_version = ">= 1.5.0"
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 5.30.0"
    }
    random = {
      source  = "hashicorp/random"
      version = "~> 3.6.0"
    }
  }
}

provider "google" {
  project = var.project_id
  region  = var.region
}

variable "project_id" {
  type    = string
  default = "fiap-project-514922"
}

variable "region" {
  type    = string
  default = "us-east1"
}

variable "environment" {
  type    = string
  default = "homolog"
}

locals {
  env_canon = var.environment == "prod" ? "production" : var.environment
  k8s_ns    = local.env_canon == "production" ? "oficina-prod" : "oficina-homolog"
}

data "terraform_remote_state" "cloudsql" {
  backend = "gcs"
  config = {
    bucket = "fiap-project-514922-tfstate"
    prefix = "platform/cloudsql"
  }
}

data "terraform_remote_state" "serverless" {
  backend = "gcs"
  config = {
    bucket = "fiap-project-514922-tfstate"
    prefix = "services/oficina-serverless/${var.environment}"
  }
}

resource "random_password" "billing_db_password" {
  length  = 24
  special = false
}

resource "google_sql_database" "billing_db" {
  name     = "oficina_billing_${local.env_canon}"
  instance = data.terraform_remote_state.cloudsql.outputs.cloud_sql_instance_name
}

resource "google_sql_user" "billing_user" {
  name     = "oficina_billing_${local.env_canon}"
  instance = data.terraform_remote_state.cloudsql.outputs.cloud_sql_instance_name
  password = random_password.billing_db_password.result
}

resource "google_service_account" "billing_sa" {
  account_id   = "oficina-billing-${local.env_canon}-sa"
  display_name = "Runtime SA para oficina-billing (${local.env_canon})"
}

resource "google_service_account_iam_member" "workload_identity" {
  service_account_id = google_service_account.billing_sa.name
  role               = "roles/iam.workloadIdentityUser"
  member             = "serviceAccount:${var.project_id}.svc.id.goog[${local.k8s_ns}/oficina-billing-sa]"
}

resource "google_project_iam_member" "billing_pubsub_publisher" {
  project = var.project_id
  role    = "roles/pubsub.publisher"
  member  = "serviceAccount:${google_service_account.billing_sa.email}"
}

resource "google_pubsub_subscription" "billing_comandos_sub" {
  name  = "oficina-${local.env_canon}-billing-comandos-sub"
  topic = "projects/${var.project_id}/topics/oficina-${local.env_canon}-billing-comandos-v1"

  ack_deadline_seconds = 30
  dead_letter_policy {
    dead_letter_topic     = "projects/${var.project_id}/topics/oficina-${local.env_canon}-billing-comandos-v1-dlq"
    max_delivery_attempts = 5
  }
}

resource "google_pubsub_subscription_iam_member" "billing_sub_reader" {
  subscription = google_pubsub_subscription.billing_comandos_sub.name
  role         = "roles/pubsub.subscriber"
  member       = "serviceAccount:${google_service_account.billing_sa.email}"
}

resource "google_secret_manager_secret" "database_url" {
  secret_id = "oficina-billing-${local.env_canon}-database-url"
  replication {
    auto {}
  }
}

resource "google_secret_manager_secret_version" "database_url" {
  secret      = google_secret_manager_secret.database_url.id
  secret_data = "postgresql+pg8000://${google_sql_user.billing_user.name}:${random_password.billing_db_password.result}@${data.terraform_remote_state.cloudsql.outputs.cloud_sql_private_ip}:5432/${google_sql_database.billing_db.name}"
}

resource "google_secret_manager_secret" "mp_access_token" {
  secret_id = "oficina-billing-${local.env_canon}-mp-access-token"
  replication {
    auto {}
  }
}

resource "google_secret_manager_secret" "mp_webhook_secret" {
  secret_id = "oficina-billing-${local.env_canon}-mp-webhook-secret"
  replication {
    auto {}
  }
}

resource "google_secret_manager_secret_iam_member" "secret_access" {
  for_each = toset([
    google_secret_manager_secret.database_url.secret_id,
    google_secret_manager_secret.mp_access_token.secret_id,
    google_secret_manager_secret.mp_webhook_secret.secret_id,
  ])
  secret_id = each.key
  role      = "roles/secretmanager.secretAccessor"
  member    = "serviceAccount:${google_service_account.billing_sa.email}"
}

output "billing_database_name" {
  value = google_sql_database.billing_db.name
}

output "billing_service_account_email" {
  value = google_service_account.billing_sa.email
}

data "google_project" "pubsub_project" {
  project_id = var.project_id
}

resource "google_pubsub_subscription_iam_member" "dead_letter_forwarder" {
  project      = var.project_id
  subscription = google_pubsub_subscription.billing_comandos_sub.name
  role         = "roles/pubsub.subscriber"
  member       = "serviceAccount:service-${data.google_project.pubsub_project.number}@gcp-sa-pubsub.iam.gserviceaccount.com"
}
