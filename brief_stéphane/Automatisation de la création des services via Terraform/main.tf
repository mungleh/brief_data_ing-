terraform {
  required_providers {
    google = {
      source = "hashicorp/google"
      version = "4.51.0"
    }
  }
}

provider "google" {
  project = "tairaformeh"
  region = "europe-west2"
  zone = "europe-west2-c"
}

locals {
    project_id = "tairaformeh"
    org_id = ""
}

resource "google_project" "demo_project" {
    name = "tairaformeh"
    project_id = local.project_id
    org_id = local.org_id
    billing_account = "012491-3DB811-031788"
  }

# Create new storage bucket in the europe-west9
# location with Standard Storage

resource "google_storage_bucket" "saissvai" {
 name          = "saissvai"
 location      = "europe-west9"
 force_destroy = true
}

resource "google_storage_bucket" "perkai" {
 name          = "perkai"
 location      = "europe-west9"
 force_destroy = true
}

resource "google_storage_bucket" "codeh" {
  name     = "codeh"
  location = "europe-west9"
  uniform_bucket_level_access = true
  force_destroy = true
}

resource "google_storage_bucket_object" "codezip" {
  name   = "code.zip"
  bucket = google_storage_bucket.codeh.name
  source = "data/code.zip"
}

# Upload a text file as an object
# to the storage bucket

resource "google_storage_bucket_object" "clients" {
 name         = "clients.csv"
 source       = "data/clients.csv"
 content_type = "text/plain"
 bucket       = google_storage_bucket.saissvai.id
}

resource "google_storage_bucket_object" "produits" {
 name         = "produits.csv"
 source       = "data/produits.csv"
 content_type = "text/plain"
 bucket       = google_storage_bucket.saissvai.id
}

resource "google_storage_bucket_object" "stocks" {
 name         = "stocks.csv"
 source       = "data/stocks.csv"
 content_type = "text/plain"
 bucket       = google_storage_bucket.saissvai.id
}

resource "google_storage_bucket_object" "ventes" {
 name         = "ventes.csv"
 source       = "data/ventes.csv"
 content_type = "text/plain"
 bucket       = google_storage_bucket.saissvai.id
}


resource "google_cloudfunctions2_function" "function" {
  name        = "phonksion"
  location    = "europe-west9"
  description = "A new phonksion"

  build_config {
    runtime    = "python310"
    entry_point = "pipi"
    source {
      storage_source {
        bucket = google_storage_bucket.codeh.name
        object = google_storage_bucket_object.codezip.name
      }
    }
  }

  service_config {
    max_instance_count  = 3
    available_memory    = "512M"
    timeout_seconds     = 120
  }
}
