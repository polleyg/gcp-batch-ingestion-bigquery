# Uses Cloud Build to deploy a scalable batch ingestion pipeline consisting of GCS, Cloud Functions, Dataflow and BigQuery
See `cloudbuild.yaml` for an idea of what this CI/CD pipeline does. Essentially it runs a container for each step
of the build using Cloud Build and deploys each component of the pipeline to GCP. It also uses Terraform to create
the initial buckets for deploying all the binaries to. See the `infra.tf` file for all that nonsense. @Graham @Phillip