provider "google" {
  project = var.project_id
  region  = "us"
}

resource "google_storage_bucket" "bronze_susep" {
  name                        = "bronze_susep"
  location                    = "US"
  storage_class               = "STANDARD"
  uniform_bucket_level_access = true
  force_destroy               = true
}

resource "google_storage_bucket" "fipe_bronze" {
  name                        = "fipe-bronze"
  location                    = "US"
  storage_class               = "STANDARD"
  uniform_bucket_level_access = true
  force_destroy               = true
}

resource "google_storage_bucket" "run_sources" {
  name                        = "run-sources-optical-victor-463515-v8-southamerica-east1"
  location                    = "SOUTHAMERICA-EAST1"
  storage_class               = "STANDARD"
  uniform_bucket_level_access = true
  force_destroy               = true
}

resource "google_storage_bucket" "seguros_bronze" {
  name                        = "seguros_bronze"
  location                    = "US"
  storage_class               = "STANDARD"
  uniform_bucket_level_access = true
  force_destroy               = true
}
