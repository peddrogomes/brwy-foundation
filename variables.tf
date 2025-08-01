locals {
    labels = {
        "data-project" = var.data-project
    }
}

variable "project" {
    type= string
    description = "Google Cloud project ID"
}

variable "region" {
    type= string
    description = "Google Cloud project region"
}

variable  "data-project" {
    type = string
    description = "Name of data pipeline project to use as resource prefix"
}

variable "subnet_name" {
    type = string
    description = "Subnet name for dataproc cluster"
    default = "default"
}
