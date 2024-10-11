## CSV TAVU
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

# CLOUDE FOKTION
resource "google_storage_bucket_object" "codezip" {
  name   = "code.zip"
  bucket = google_storage_bucket.codeh.name
  source = "data/code.zip"
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
