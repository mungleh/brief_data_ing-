terraform {
  required_providers {
    google = {
      source = "hashicorp/google"
      version = "4.51.0"
    }
  }
}

provider "google" {
  project = "tairaformehe"
  region = "europe-west2"
  zone = "europe-west2-c"
}

locals {
    project_id = "tairaformehe"
    org_id = ""
}

resource "google_project_service" "cloud_run_api" {
  service = "run.googleapis.com"
}

resource "google_project" "project" {
    name = "tairaformehe"
    project_id = local.project_id
    org_id = local.org_id
    billing_account = "012491-3DB811-031788"
  }
