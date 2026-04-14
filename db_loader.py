import pandas as pd
from sqlalchemy import create_engine, text
from config import DB_URI, TABLE_KEYS
import os

engine = create_engine(DB_URI)

def clean_columns(df):
    """Normalize headers to lowercase and strip whitespace."""
    df.columns = [str(c).strip().lower().replace(" ", "_").replace("/", "_") for c in df.columns]
    # Drop completely unnamed/empty trailing columns typical in NSE legacy CSVs
    df = df.loc[:, ~df.columns.str.contains('^unnamed')]
    return df

def generate_upsert_query(table_name, df, primary_keys):
    """Creates a raw Postgres ON CONFLICT query."""
    columns = list(df.columns)
    update_cols = [c for c in columns if c not in primary_keys]
    
    col_str = ", ".join([f'"{c}"' for c in columns])
    val_str = ", ".join([f"%({c})s" for c in columns])
    
    query = f'INSERT INTO "{table_name}" ({col_str}) VALUES ({val_str})'
    
    if update_cols and primary_keys:
        conflict_target = ", ".join([f'"{pk}"' for pk in primary_keys])
        update_set = ", ".join([f'"{c}" = EXCLUDED."{c}"' for c in update_cols])
        query += f' ON CONFLICT ({conflict_target}) DO UPDATE SET {update_set}'
    elif primary_keys:
        conflict_target = ", ".join([f'"{pk}"' for pk in primary_keys])
        query += f' ON CONFLICT ({conflict_target}) DO NOTHING'

    return query

def load_to_postgres(file_path, table_name, trade_date_str):
    try:
        # STRICT DISCARD RULE: If it's not in the map, ignore it completely.
        if table_name not in TABLE_KEYS:
            return False, "File explicitly discarded / unmapped in config."

        if file_path.lower().endswith(('.csv', '.txt', '.dat', '.csv.gz')):
            sep = ',' if not file_path.lower().endswith('.txt') else '\t'
            try:
                df = pd.read_csv(file_path, sep=sep, low_memory=False)
            except Exception:
                df = pd.read_csv(file_path, sep=',', low_memory=False)
        else:
            return False, f"Unsupported extension for DB load: {file_path}"
            
        df = clean_columns(df)
        
        # Enforce trade_date globally
        df['trade_date'] = pd.to_datetime(trade_date_str).date()

        keys = TABLE_KEYS.get(table_name) 

        with engine.connect() as conn:
            # 1. Create table structure if it doesn't exist by inserting 0 rows
            df.head(0).to_sql(table_name, engine, if_exists='append', index=False)
            
            # 2. Enforce primary key on the table if we just created it
            try:
                pk_str = ", ".join([f'"{k}"' for k in keys])
                conn.execute(text(f'ALTER TABLE "{table_name}" ADD PRIMARY KEY ({pk_str});'))
                conn.commit()
            except Exception:
                conn.rollback() # Key likely already exists
                
            # 3. Execute bulk upsert
            records = df.to_dict(orient='records')
            if records:
                upsert_query = generate_upsert_query(table_name, df, keys)
                conn.execute(text(upsert_query), records)
                conn.commit()
            
        return True, f"Loaded {len(df)} rows"
    except Exception as e:
        return False, str(e)