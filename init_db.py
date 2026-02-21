from sqlalchemy import create_engine, text

# 資料庫連線字串 (使用您在 docker-compose 設定的密碼)
# TrustServerCertificate=yes 是因為 Docker 本地連線不需要嚴格的 SSL 憑證
DB_USER = "sa"
DB_PASS = "SectorFlux_DB_2026!"
DB_HOST = "localhost"
DB_PORT = "1433"

# 1. 先連線到預設的 master 資料庫，用來建立新的 Database
master_engine = create_engine(
    f"mssql+pyodbc://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/master?driver=ODBC+Driver+18+for+SQL+Server&TrustServerCertificate=yes",
    isolation_level="AUTOCOMMIT" # 建立 DB 需要 AUTOCOMMIT 模式
)

def setup_database():
    try:
        with master_engine.connect() as conn:
            # 建立 SectorFlux 專屬資料庫
            conn.execute(text("IF NOT EXISTS (SELECT * FROM sys.databases WHERE name = 'SectorFluxDB') CREATE DATABASE SectorFluxDB;"))
            print("✅ Database 'SectorFluxDB' 檢查/建立完成！")
            
    except Exception as e:
        print(f"建立 Database 時發生錯誤: {e}")
        return False
    return True

# 2. 連線到剛建好的 SectorFluxDB，準備建立 Table
def setup_tables():
    sectorflux_engine = create_engine(
        f"mssql+pyodbc://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/SectorFluxDB?driver=ODBC+Driver+18+for+SQL+Server&TrustServerCertificate=yes"
    )
    
    # 建立日線價格事實表 (Fact Table) 的 DDL
    create_table_sql = """
    IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='Fact_DailyPrice' AND xtype='U')
    CREATE TABLE Fact_DailyPrice (
        Date DATE NOT NULL,
        Symbol VARCHAR(20) NOT NULL,
        [Open] FLOAT,
        High FLOAT,
        Low FLOAT,
        [Close] FLOAT,
        Volume BIGINT,
        PRIMARY KEY (Date, Symbol) -- 複合主鍵，確保同一天同一檔股票不會重複
    );
    """
    
    try:
        with sectorflux_engine.connect() as conn:
            conn.execute(text(create_table_sql))
            conn.commit()
            print("✅ Table 'Fact_DailyPrice' 檢查/建立完成！ Schema 設定完畢。")
    except Exception as e:
        print(f"建立 Table 時發生錯誤: {e}")

if __name__ == "__main__":
    if setup_database():
        setup_tables()