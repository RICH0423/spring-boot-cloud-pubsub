# spring-boot-cloud-pubsub
 This repo demonstrates how to use the Spring Boot &amp; GCP Pub/Sub


## Libraries
Maven libraries used in this project which include  HAPI FHIR core and server library.
- [spring-cloud-gcp-starter-pubsub](https://cloud.google.com/pubsub/docs/quickstart-client-libraries)

## Getting Started
This is an example of how you may give instructions on setting up your project locally.

### Setup
- To run this code sample, you must have a Google Cloud Platform project with billing and the Google
Cloud Pub/Sub API enabled.

Install and initialize the https://cloud.google.com/sdk/[Google Cloud SDK].
Log in with application default credentials using the following command:

```
gcloud auth application-default login
```

### Build and run

- to compile and build war:

```
mvn clean package
```

- run the app
```
mvn spring-boot:run
```
