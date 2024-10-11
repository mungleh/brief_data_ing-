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
