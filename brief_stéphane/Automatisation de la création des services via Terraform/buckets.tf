resource "google_storage_bucket" "saissvais" {
 name          = "saissvais"
 location      = "europe-west9"
 force_destroy = true
}

resource "google_storage_bucket" "perkais" {
 name          = "perkais"
 location      = "europe-west9"
 force_destroy = true
}

resource "google_storage_bucket" "codehs" {
  name     = "codehs"
  location = "europe-west9"
  uniform_bucket_level_access = true
  force_destroy = true
}
