FROM python:3.10-slim

WORKDIR /app

# Installer dépendances
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copier le code
COPY src ./src

# Commande par défaut
CMD ["python", "-m", "src.pipeline"]
