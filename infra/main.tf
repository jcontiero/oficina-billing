terraform {
  required_version = ">= 1.5.0"
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 5.0"
    }
  }
}

variable "project_id" {
  type    = string
  default = "fiap-project-514922"
}

variable "environment" {
  type    = string
  default = "homolog"
}

# Recursos exclusivos do serviço oficina-billing (banco SQL lógico, usuário exclusivo e subscriptions)
# Deploys e provisionamento permanecem bloqueados até a autorização na Etapa P7.
output "billing_database_name" {
  value = "oficina_billing_${var.environment}"
}
