# 使用輕量級的 Python 3.10 映像檔
FROM python:3.10-slim

# 設定工作目錄
WORKDIR /workspace

# 安裝系統依賴與 Microsoft ODBC Driver 18 (這是 pyodbc 能連線的關鍵)
RUN apt-get update && apt-get install -y \
    curl apt-transport-https gnupg2 build-essential unixodbc-dev \
    # 使用 gpg --dearmor 取代已經被廢棄的 apt-key
    && curl -fsSL https://packages.microsoft.com/keys/microsoft.asc | gpg --dearmor -o /usr/share/keyrings/microsoft-prod.gpg \
    # 將來源指向 debian/12
    && curl -fsSL https://packages.microsoft.com/config/debian/12/prod.list > /etc/apt/sources.list.d/mssql-release.list \
    && apt-get update \
    && ACCEPT_EULA=Y apt-get install -y msodbcsql18 \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# 複製並安裝 Python 套件
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# 複製專案原始碼到容器內
COPY . .

# 讓容器保持運行狀態 (背景待命)，以便我們用 crontab 呼叫它
CMD ["tail", "-f", "/dev/null"]