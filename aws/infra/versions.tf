terraform {
  required_version = ">= 1.6"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 6.0"
    }
    archive = {
      source  = "hashicorp/archive"
      version = "~> 2.4"
    }
    random = {
      source  = "hashicorp/random"
      version = "~> 3.6"
    }
  }

  # State is kept locally (terraform.tfstate, git-ignored) because this is a
  # single-person demo that is created and destroyed in the same session.
  # For team use, switch to an S3 backend with native locking:
  # backend "s3" { bucket = "..." key = "clockdata/terraform.tfstate" region = "eu-west-3" use_lockfile = true }
}
