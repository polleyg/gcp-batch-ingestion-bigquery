# Uses Cloud Build to deploy a scalable batch ingestion pipeline consisting of GCS, Cloud Functions, Dataflow and BigQuery
See `cloudbuild.yaml` for an idea of what this CI/CD pipeline does. Essentially it runs a container for each step
of the build using Cloud Build and deploys each component of the pipeline to GCP. It also uses Terraform to create
the initial buckets for deploying all the binaries to. See the `infra.tf` file for all that nonsense.

Here's what it looks like:

<img width="896" alt="screen shot 2018-09-04 at 5 33 17 pm" src="https://user-images.githubusercontent.com/5554342/45016683-b84de480-b068-11e8-8f8b-f729d78e69b1.png">
