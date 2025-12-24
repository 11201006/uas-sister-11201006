FROM python:3.11-slim
WORKDIR /app
RUN pip install requests
COPY generator.py .
CMD ["python", "generator.py"]
