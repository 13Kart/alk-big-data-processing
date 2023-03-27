FROM gcr.io/dataflow-templates-base/python38-template-launcher-base

ARG WORKDIR=/dataflow/template
RUN mkdir -p ${WORKDIR}
WORKDIR ${WORKDIR}

COPY wordcount_bq.py .

ENV FLEX_TEMPLATE_PYTHON_PY_FILE="${WORKDIR}/wordcount_bq.py"

RUN pip install apache-beam[gcp]
