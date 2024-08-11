# Use an appropriate base image
FROM jupyter/base-notebook

# Install tini
USER root
RUN apt-get update && apt-get install -y tini

# Verify tini installation
RUN which tini

# Set PATH environment variable
ENV PATH="/usr/bin:${PATH}"

# Copy the start-notebook.sh script to the container
COPY start-notebook.sh /usr/local/bin/start-notebook.sh

# Make the script executable
RUN chmod +x /usr/local/bin/start-notebook.sh

# Use the full path to tini in ENTRYPOINT
ENTRYPOINT ["/usr/bin/tini", "--"]
CMD ["/usr/local/bin/start-notebook.sh", "--NotebookApp.token=''", "--NotebookApp.password=''"]
