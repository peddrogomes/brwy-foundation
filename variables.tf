locals {
    labels = {
        "data-project" = var.data-project
    }
    
    service_account = var.branch-hash == "" ? "966844133549-compute@developer.gserviceaccount.com" : "536022346875-compute@developer.gserviceaccount.com"
    

    enable_delete_protection = var.branch-hash == ""? false : true
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
