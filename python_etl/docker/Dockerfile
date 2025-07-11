FROM python:3.11-slim

# Define o diretório de trabalho no container
WORKDIR /app

# Instala pacotes adicionais necessários
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    python3-dev \
    && rm -rf /var/lib/apt/lists/*

# Copia apenas o arquivo de requisitos primeiro para aproveitar o cache do Docker
COPY requirements.txt .

# Instala as dependências Python
RUN pip install --no-cache-dir -r requirements.txt

# Copia o código da aplicação para o container
# Isso é feito após a instalação das dependências para aproveitar o cache
COPY . .

# Define variáveis de ambiente para Python
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

# Comando para rodar o script ETL
CMD ["python", "etl.py"]

