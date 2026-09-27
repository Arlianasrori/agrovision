from psycopg_pool import AsyncConnectionPool

async def execute_write_query(pool: AsyncConnectionPool, query: str, params: tuple) -> bool:
    """Mengeksekusi query tulis (INSERT/UPDATE/DELETE) yang reusable."""
    try:
        async with pool.connection() as conn:
            async with conn.cursor() as cur:
                await cur.execute(query, params)
                await conn.commit()
        return True
    except Exception as e:
        print(f"Database error saat eksekusi query: {e}")
        return False