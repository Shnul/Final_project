# Use the official Elasticsearch image from Elastic
FROM docker.elastic.co/elasticsearch/elasticsearch:7.10.1

# Set environment variables for Elasticsearch
ENV discovery.type=single-node

# Expose the default Elasticsearch port
EXPOSE 9200

# Run Elasticsearch
CMD ["elasticsearch"]