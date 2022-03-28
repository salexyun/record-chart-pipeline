locals {
  data_lake_bucket = "data_lake"
}

variable "project" {
  description = "GCP project ID."
}

# Ref: https://cloud.google.com/about/locations
variable "region" {
  default     = "northamerica-northeast1"
  description = "Google Compute Engine region."
}

# variable "zone" {
#   default     = "northamerica-northeast1-a"
#   description = "Google Compute Engine zone"
# }

variable "storage_class" {
  default     = "STANDARD"
  description = "Google Cloud Storage bucket classes."
}

variable "bq_dataset" {
  default     = "billboard_200"
  type        = string
  description = "Raw data in cloud storage will be organized as tables in BigQuery."
}
