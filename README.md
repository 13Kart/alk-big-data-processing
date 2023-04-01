# GFT&Google Cloud - Cloud solutions in practise - Big Data processing

## beam

### Local development

```bash
python3 -m venv /path/to/new/virtual/environment
source /path/to/new/virtual/environment/bin/activate
pip install -r requirements.txt
```

### Create Dataflow flex template

- Set up variables
```bash
export REGION="<your-region>"
export PROJECT_ID="<your-project-id>"
export REPOSITORY="<your-artifact-registry-repository>"
export TEMPLATE_NAME="wordcount_bq"
export TAG="${REGION}-docker.pkg.dev/${PROJECT_ID}/${REPOSITORY}/${TEMPLATE_NAME}:latest"
export BUCKET_NAME="<your-bucket-name>"
```
- Push container image to Artifact Registry
```bash
gcloud builds submit --tag ${TAG} .
```
- Create a flex template
```bash
  gcloud dataflow flex-template build gs://${BUCKET_NAME}/dataflow-flex-templates/${TEMPLATE_NAME}.json \
     --image ${TAG} \
     --sdk-language "PYTHON" \
     --metadata-file "metadata.json"
```

