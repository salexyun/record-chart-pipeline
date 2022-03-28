# Terraform

## Setup
* Download [Terraform](https://www.terraform.io/downloads)
* Change `region` variable in `variables.tf` according to your location

## Execution
```shell
# Initialize a working directory with Terraform configuration/state file
terraform init

# Preview the changes to the infrastructure plan
terraform plan -var="project=<your-gcp-project-id>"

# Executes the above proposed plan
terraform apply -var="project=<your-gcp-prject-id>"
```