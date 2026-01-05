FROM machine-learning-model:latest

WORKDIR /app

# Notebook + Grid ins Image
COPY notebooks/ /app/notebooks/

# optional: JupyterLab Extension Manager readonly => weniger Warnungen
ENV JUPYTERLAB_SETTINGS_DIR=/root/.jupyter/lab/user-settings

EXPOSE 8888
