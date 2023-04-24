FROM quay.io/astronomer/astro-runtime-dev:8.0.0-alpha2

ENV AIRFLOW__CORE__ALLOWED_DESERIALIZATION_CLASSES = airflow\.* astro\.*