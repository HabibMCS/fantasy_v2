import pyodbc
import os
from dotenv import load_dotenv

def cleanup_connections():
    """Clean up all existing connections to the database"""
    try:
        load_dotenv()
        
        # Create a connection to master database
        connection_string = (
            "Driver={ODBC Driver 18 for SQL Server};"
            "Server=tcp:34.237.12.44,1433;"
            "Database=master;"
            "UID=%s;"
            "PWD=%s;"
            "TrustServerCertificate=yes;"
            "Encrypt=yes;"
        ) % (os.getenv('ODBC_DB_USER'), os.getenv('ODBC_DB_PASS'))
        
        conn = pyodbc.connect(connection_string, autocommit=True)  # Enable autocommit
        cursor = conn.cursor()
        
        # First, get database ID
        cursor.execute("SELECT DB_ID('ds-cbssports')")
        db_id = cursor.fetchone()[0]
        
        if not db_id:
            print("Database not found")
            return
            
        # Get all processes except our current session
        cursor.execute("""
            SELECT 
                spid,
                status,
                loginame,
                hostname,
                program_name
            FROM master.dbo.sysprocesses 
            WHERE dbid = ? 
            AND spid != @@SPID
        """, (db_id,))
        
        processes = cursor.fetchall()
        count = 0
        
        print("\nActive connections:")
        for process in processes:
            print(f"SPID: {process.spid}, Status: {process.status}, Login: {process.loginame}")
            print(f"Host: {process.hostname}, Program: {process.program_name}\n")
        
        # Try to close connections gracefully first
        cursor.execute("""
            ALTER DATABASE [ds-cbssports] 
            SET SINGLE_USER 
            WITH ROLLBACK IMMEDIATE
        """)
        print("Database set to single user mode")
        
        # Reset to multi-user
        cursor.execute("""
            ALTER DATABASE [ds-cbssports] 
            SET MULTI_USER
        """)
        print("Database reset to multi-user mode")
        
        # Verify connections are cleared
        cursor.execute("""
            SELECT COUNT(*) 
            FROM master.dbo.sysprocesses 
            WHERE dbid = ? 
            AND spid != @@SPID
        """, (db_id,))
        
        remaining = cursor.fetchone()[0]
        print(f"\nRemaining connections: {remaining}")
        
        # Close our connection
        cursor.close()
        conn.close()
        print("Cleanup complete")
        
    except Exception as err:
        print(f"Error during cleanup: {err}")
        if 'cursor' in locals():
            cursor.close()
        if 'conn' in locals():
            conn.close()

if __name__ == "__main__":
    cleanup_connections()