FROM astrocrpublic.azurecr.io/runtime:3.1-13

USER root
RUN uv pip install --system --extra-index-url https://pypi.org/simple/ \
    setuptools \
    apache-airflow-providers-postgres \
    soda-core-postgres \
    pandas \
    requests
USER astro