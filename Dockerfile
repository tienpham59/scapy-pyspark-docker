# Sử dụng image Python chính thức
FROM python:3.12-slim

# Đặt thư mục làm việc trong container
WORKDIR /app

# Cài đặt các công cụ cần thiết
RUN apt-get update && \
    apt-get install -y wget curl gnupg2 && \
    apt-get install -y --no-install-recommends \
    build-essential \
    libssl-dev \
    libffi-dev \
    libxml2-dev \
    libxslt1-dev \
    zlib1g-dev \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

# Sao chép file requirements.txt vào container
COPY requirements.txt .

# Cài đặt các gói phụ thuộc
RUN pip install --no-cache-dir -r requirements.txt

# Sao chép mã nguồn vào container
COPY . .

# Expose MongoDB port
EXPOSE 27017
EXPOSE 5432
CMD ["sh", "-c", "sleep 30 && python -m scrapy crawl chothuenha_spider -a start_page=1 -a end_page=500"]
