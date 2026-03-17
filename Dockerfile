FROM python:3.11-slim

# 設定工作目錄
WORKDIR /workspace

# 安裝系統依賴與 Microsoft ODBC Driver 18 
RUN apt-get update && apt-get install -y \
    git \
    curl apt-transport-https gnupg2 build-essential unixodbc-dev \
    && curl -fsSL https://packages.microsoft.com/keys/microsoft.asc | gpg --dearmor -o /usr/share/keyrings/microsoft-prod.gpg \
    && curl -fsSL https://packages.microsoft.com/config/debian/12/prod.list > /etc/apt/sources.list.d/mssql-release.list \
    && apt-get update \
    && ACCEPT_EULA=Y apt-get install -y msodbcsql18 \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# 複製並安裝 Python 套件
RUN pip install --upgrade pip
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# 複製專案原始碼到容器內
COPY . .

# 讓容器保持運行狀態 (背景待命)，以便我們用 crontab 呼叫它
CMD ["tail", "-f", "/dev/null"]