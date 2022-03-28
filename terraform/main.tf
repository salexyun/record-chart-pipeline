# Terraform settings
terraform {
  required_version = ">=1.0"
  backend "local" {} # stores state on the local filesystem; could specify gcs instead
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = ">=3.5"
    }
  }
}

# Provider configuration
provider "google" {
  project = var.project
  region  = var.region
}

# Data lake
resource "google_storage_bucket" "data-lake-bucket" {
  name     = "${local.data_lake_bucket}_${var.project}"
  location = var.region

  storage_class               = var.storage_class
  uniform_bucket_level_access = true

  versioning {
    enabled = true
  }

  lifecycle_rule {
    action {
      type = "Delete"
    }
    condition {
      age = 30 # days
    }
  }

  force_destroy = true
}

# Data warehouse
resource "google_bigquery_dataset" "dataset" {
  dataset_id = var.bq_dataset
  project    = var.project
  location   = var.region
}
