terraform {
  backend "gcs" {
    bucket  = "tf-state-gcp-batch-ingestion"
    region  = "australia-southeast1-a"
    prefix  = "terraform/state"
  }
}

provider "google" {
  project     = "grey-sort-challenge"
  region      = "australia-southeast1-a"
}

# Create a new instance
resource "google_compute_instance" "ubuntu-xenial" {
   name = "ubuntu-xenial"
   machine_type = "f1-micro"
   zone = "australia-southeast1-a"
   boot_disk {
      initialize_params {
      image = "ubuntu-1604-lts"
   }
}
network_interface {
   network = "default"
   access_config {}
}
service_account {
   scopes = ["userinfo-email", "compute-ro", "storage-ro"]
   }
}