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

variable "branch-hash" {
    type = string
    description = "Hash of the branch name to append to resource names for non-main branches"
    default = ""
}

variable "subnet_name" {
    type = string
    description = "Subnet name for dataproc cluster"
    default = "default"
}
