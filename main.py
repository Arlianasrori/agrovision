import os
from typing import Optional
from contextlib import asynccontextmanager
from typing import Any
import random
from datetime import datetime, timedelta

from psycopg_pool import AsyncConnectionPool
from psycopg.rows import dict_row
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, Query, Request, status, Depends
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field
import jwt

from fastapi_mqtt import FastMQTT, MQTTConfig
from gmqtt import Client as MQTTClient

load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL")
# Secret key untuk JWT (ubah dengan string acak yang aman di production)
JWT_SECRET_KEY = os.getenv("JWT_SECRET_KEY", "agrovision_secret_key_2026")
JWT_ALGORITHM = "HS256"
JWT_EXPIRATION_HOURS = 24

if not DATABASE_URL:
    raise RuntimeError(
        "DATABASE_URL belum ditemukan. Periksa file .env."
    )

random_hex = f"{random.randrange(16**6):06x}"
client_id = f"mqtt_fastapi_{random_hex}"

mqtt_config = MQTTConfig(
    host="broker.emqx.io",
    port=1883,               
    keepalive=60,
    client_id=client_id,
    version=4,               
    clean_session=True,      
)

fast_mqtt = FastMQTT(config=mqtt_config)
security = HTTPBearer()

db_pool: AsyncConnectionPool = None
last_saved_time: dict[str, datetime] = {}
SAVE_INTERVAL_MINUTES = 30

@asynccontextmanager
async def lifespan(app: FastAPI):
    global db_pool
    
    async with AsyncConnectionPool(
        conninfo=DATABASE_URL, 
        kwargs={"row_factory": dict_row}
    ) as pool:
        db_pool = pool
        app.state.pool = pool
        
        print("Starting MQTT connection...")
        await fast_mqtt.mqtt_startup()
        
        yield
        
        print("Shutting down MQTT...")
        await fast_mqtt.mqtt_shutdown()

# Zona waktu aplikasi
APP_TIMEZONE = "Asia/Jakarta"

app = FastAPI(
    title="AGROVISION API",
    version="1.3.0",
    description=(
        "Backend AGROVISION dengan autentikasi JWT, "
        "MQTT Handler, dan manajemen lahan pertanian."
    ),
    lifespan=lifespan
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://127.0.0.1:5173",
        "http://localhost:5173",
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# ==================== PYDANTIC MODELS ====================
class LoginRequest(BaseModel):
    username: str
    password: str


# ==================== AUTH HELPER & DEPENDENCY ====================
def create_access_token(data: dict):
    to_encode = data.copy()
    expire = datetime.utcnow() + timedelta(hours=JWT_EXPIRATION_HOURS)
    to_encode.update({"exp": expire})
    encoded_jwt = jwt.encode(to_encode, JWT_SECRET_KEY, algorithm=JWT_ALGORITHM)
    return encoded_jwt


async def get_current_user(credentials: HTTPAuthorizationCredentials = Depends(security)):
    """Dependency untuk memverifikasi token JWT pada Header Authorization."""
    token = credentials.credentials
    try:
        payload = jwt.decode(token, JWT_SECRET_KEY, algorithms=[JWT_ALGORITHM])
        return payload  # Berisi data petani (id, username, nama)
    except jwt.ExpiredSignatureError:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Token sudah kedaluwarsa. Silakan login kembali."
        )
    except jwt.PyJWTError:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Token tidak valid."
        )


# ==================== MQTT HANDLERS ====================
@fast_mqtt.on_connect()
async def connect(client: MQTTClient, flags: int, rc: int, properties: Any):
    print("Connected to MQTT Broker!")
    if db_pool:
        try:
            async with db_pool.connection() as conn:
                async with conn.cursor() as cur:
                    await cur.execute("SELECT kode FROM sensor_petani")
                    sensors = await cur.fetchall()
                    
                    for sensor in sensors:
                        kode = sensor["kode"]
                        topic = f"agrovision2026_{kode}"
                        client.subscribe(topic)
                        print(f"Subscribed to topic: {topic}")
        except Exception as e:
            print(f"Gagal melakukan subscribe otomatis dari database: {e}")

@fast_mqtt.on_message()
async def message(client: MQTTClient, topic: str, payload: bytes, qos: int, properties: Any):
    payload_str = payload.decode().strip()

    if topic.startswith("agrovision2026_"):
        kode_sensor = topic.replace("agrovision2026_", "", 1)
        now = datetime.now()

        if kode_sensor in last_saved_time:
            time_diff = now - last_saved_time[kode_sensor]
            if time_diff < timedelta(minutes=SAVE_INTERVAL_MINUTES):
                return 0

        try:
            value = float(payload_str)
            if db_pool:
                async with db_pool.connection() as conn:
                    async with conn.cursor() as cur:
                        await cur.execute(
                            """
                            INSERT INTO sensor_data (id_sensor, value, datetime) 
                            VALUES (%s, %s, NOW())
                            """,
                            (kode_sensor, value)
                        )
                        await conn.commit()
                        last_saved_time[kode_sensor] = now
        except ValueError:
            pass
        except Exception as e:
            print(f"Gagal menyimpan data ke database: {e}")

    return 0


# ==================== REST API ENDPOINTS ====================

@app.post("/api/auth/login", summary="Login Petani & Generate JWT")
async def login(data: LoginRequest, request: Request):
    """Endpoint login untuk mendapatkan access_token JWT."""
    pool = request.app.state.pool
    async with pool.connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                "SELECT id, nama, asal, username FROM petani WHERE username = %s AND password = %s",
                (data.username, data.password)
            )
            petani = await cur.fetchone()
            
            if not petani:
                raise HTTPException(
                    status_code=status.HTTP_401_UNAUTHORIZED,
                    detail="Username atau password salah."
                )
            
            # Buat payload token berdasarkan data petani
            token_data = {
                "id": petani["id"],
                "username": petani["username"],
                "nama": petani["nama"]
            }
            access_token = create_access_token(token_data)
            
            return {
                "message": "Login berhasil",
                "access_token": access_token,
                "token_type": "bearer",
                "data": petani
            }


@app.get("/api/petani/profile", summary="Get Profil Petani yang Sedang Login")
async def get_my_profile(current_user: dict = Depends(get_current_user), request: Request = None):
    """Endpoint aman untuk melihat profil petani berdasarkan token JWT yang aktif."""
    pool = request.app.state.pool
    async with pool.connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                "SELECT id, nama, asal, username FROM petani WHERE id = %s",
                (current_user["id"],)
            )
            petani = await cur.fetchone()
            return {
                "message": "Berhasil mengambil profil",
                "data": petani
            }


@app.get("/api/petani/lokasi", summary="Get Lahan / Lokasi Pertanian Petani yang Sedang Login")
async def get_my_lokasi_pertanian(current_user: dict = Depends(get_current_user), request: Request = None):
    """Endpoint aman untuk mengambil daftar lahan milik petani yang sedang login menggunakan token JWT."""
    pool = request.app.state.pool
    async with pool.connection() as conn:
        async with conn.cursor() as cur:
            # Menggunakan ID petani dari token JWT aktif
            await cur.execute(
                """
                SELECT id, nama_lahan, lat, long, provinsi, kabupaten, kecamatan, desa, detail_tempat 
                FROM lokasi_pertanian 
                WHERE id_petani = %s
                """,
                (current_user["id"],)
            )
            lokasi_list = await cur.fetchall()
            
            return {
                "message": "Berhasil mengambil data lokasi pertanian",
                "total": len(lokasi_list),
                "data": lokasi_list
            }


@app.get("/api/lokasi-pertanian/{id_lokasi}", summary="Get Detail Lokasi Pertanian Berdasarkan ID")
async def get_lokasi_pertanian_by_id(id_lokasi: int, _: dict = Depends(get_current_user), request: Request = None):
    """Endpoint aman untuk mengambil detail satu lokasi pertanian berdasarkan ID."""
    pool = request.app.state.pool
    async with pool.connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                """
                SELECT 
                    l.id, 
                    l.id_petani, 
                    p.nama AS nama_petani, 
                    l.nama_lahan, 
                    l.lat, 
                    l.long, 
                    l.provinsi, 
                    l.kabupaten, 
                    l.kecamatan, 
                    l.desa, 
                    l.detail_tempat 
                FROM lokasi_pertanian l
                JOIN petani p ON l.id_petani = p.id
                WHERE l.id = %s
                """,
                (id_lokasi,)
            )
            lokasi = await cur.fetchone()
            
            if not lokasi:
                raise HTTPException(
                    status_code=404,
                    detail="Data lokasi pertanian tidak ditemukan."
                )
            
            return {
                "message": "Berhasil mengambil data lokasi pertanian",
                "data": lokasi
            }
        
@app.get("/api/lokasi-pertanian/{id_lokasi}/riwayat", summary="Get Riwayat Sensor Berdasarkan Lokasi & Tanggal")
async def get_riwayat_sensor_by_lokasi(
    id_lokasi: int, 
    tanggal: Optional[str] = Query(None, description="Format YYYY-MM-DD, contoh: 2026-09-24"),
    current_user: dict = Depends(get_current_user), 
    request: Request = None
):
    pool = request.app.state.pool
    async with pool.connection() as conn:
        async with conn.cursor() as cur:
            # 1. Validasi hak akses lokasi pertanian milik petani yang login
            await cur.execute(
                "SELECT id FROM lokasi_pertanian WHERE id = %s AND id_petani = %s",
                (id_lokasi, current_user["id"])
            )
            lokasi = await cur.fetchone()
            if not lokasi:
                raise HTTPException(
                    status_code=status.HTTP_403_FORBIDDEN,
                    detail="Anda tidak memiliki akses ke lokasi pertanian ini."
                )

            # 2. Query dasar untuk mengambil data riwayat beserta tipe sensornya
            query = """
                SELECT 
                    sd.id,
                    sd.datetime,
                    sp.kode AS kode_sensor,
                    sp.type AS sensor_type,
                    sp.unit,
                    sd.value
                FROM lokasi_pertanian lp
                JOIN sensor_petani sp ON lp.id = sp.id_lokasi
                JOIN sensor_data sd ON sp.kode = sd.kode_sensor
                WHERE lp.id = %s
            """
            params = [id_lokasi]

            if tanggal:
                query += " AND DATE(sd.datetime) = %s"
                params.append(tanggal)

            query += " ORDER BY sd.datetime DESC"

            await cur.execute(query, tuple(params))
            rows = await cur.fetchall()

            # 3. Variabel penampung untuk kalkulasi rata-rata murni di backend
            stats_buckets = {
                "kelembapan": [],
                "suhu": [],
                "ph": [],
                "nutrisi": []
            }

            formatted_data = []
            for row in rows:
                dt = row["datetime"]
                date_str = dt.strftime("%d %b %Y")
                time_str = dt.strftime("%H:%M")
                
                sensor_type = row["sensor_type"]
                val = row["value"]
                unit = row["unit"] or ""

                # Masukkan nilai ke bucket yang tepat untuk dihitung rata-ratanya nanti
                if sensor_type in ["SOIL_MOISTURE", "AIR_MOISTURE"]:
                    stats_buckets["kelembapan"].append(val)
                elif sensor_type in ["SOIL_TEMP", "AIR_TEMP"]:
                    stats_buckets["suhu"].append(val)
                elif sensor_type == "PH":
                    stats_buckets["ph"].append(val)
                elif sensor_type in ["EC", "NITROGEN", "PHOSPHORUS", "POTASSIUM"]:
                    stats_buckets["nutrisi"].append(val)

                category_map = {
                    "SOIL_MOISTURE": "Kelembapan Tanah", "AIR_MOISTURE": "Kelembapan Tanah",
                    "SOIL_TEMP": "Suhu", "AIR_TEMP": "Suhu",
                    "PH": "pH", "EC": "Nutrisi",
                    "NITROGEN": "Nutrisi", "PHOSPHORUS": "Nutrisi", "POTASSIUM": "Nutrisi"
                }
                
                ideal_map = {
                    "SOIL_MOISTURE": "60–75%", "SOIL_TEMP": "24–30 °C",
                    "PH": "6,0–6,8", "EC": "1,4–2,0",
                    "NITROGEN": "1,2–2,0", "PHOSPHORUS": "—", "POTASSIUM": "1,5–2,5",
                    "AIR_MOISTURE": "60–80%", "AIR_TEMP": "25–33 °C"
                }

                cat = category_map.get(sensor_type, "Lainnya")
                ideal_text = ideal_map.get(sensor_type, "—")
                
                status_teks = "Optimal"
                badge_class = "rw-badge--ok"

                if sensor_type == "PH" and (val < 6.0 or val > 6.8):
                    status_teks = "Perlu perhatian"
                    badge_class = "rw-badge--warn"
                elif sensor_type == "SOIL_MOISTURE" and val < 60:
                    status_teks = "Perlu perhatian"
                    badge_class = "rw-badge--warn"

                formatted_data.append({
                    "id": row["id"],
                    "date": date_str,
                    "time": time_str,
                    "sensor": sensor_type.replace("_", " ").title(),
                    "value": f"{val} {unit}".strip(),
                    "ideal": ideal_text,
                    "status": status_teks,
                    "badgeClass": badge_class,
                    "category": cat
                })

            # 4. Hitung rata-rata aman menggunakan Python
            def calc_avg(arr, unit_default):
                if not arr:
                    return {"value": "—", "unit": unit_default}
                return {"value": str(round(sum(arr) / len(arr), 1)).replace('.', ','), "unit": unit_default}

            summary_averages = {
                "kelembapan": calc_avg(stats_buckets["kelembapan"], "%"),
                "suhu": calc_avg(stats_buckets["suhu"], "°C"),
                "ph": calc_avg(stats_buckets["ph"], ""),
                "nutrisi": calc_avg(stats_buckets["nutrisi"], "g/kg")
            }

            return {
                "message": "Berhasil mengambil data riwayat sensor",
                "total": len(formatted_data),
                "summary": summary_averages,
                "data": formatted_data
            }


@app.get("/api/lokasi-pertanian/{id_lokasi}/analisis", summary="Get Data Analitik & Grafik Sensor Berdasarkan Rentang Waktu")
async def get_analisis_lahan_by_lokasi(
    id_lokasi: int, 
    range: Optional[str] = Query("7 hari", description="Rentang waktu: 24 jam, 7 hari, 30 hari"),
    current_user: dict = Depends(get_current_user), 
    request: Request = None
):
    pool = request.app.state.pool
    async with pool.connection() as conn:
        async with conn.cursor() as cur:
            # 1. Validasi hak akses lokasi pertanian milik petani yang login
            await cur.execute(
                "SELECT id FROM lokasi_pertanian WHERE id = %s AND id_petani = %s",
                (id_lokasi, current_user["id"])
            )
            lokasi = await cur.fetchone()
            if not lokasi:
                raise HTTPException(
                    status_code=status.HTTP_403_FORBIDDEN,
                    detail="Anda tidak memiliki akses ke lokasi pertanian ini."
                )

            # 2. Tentukan interval waktu SQL berdasarkan parameter range query
            interval_sql = "7 days"
            if range == "24 jam":
                interval_sql = "24 hours"
            elif range == "30 hari":
                interval_sql = "30 days"

            # 3. Ambil data historis sensor dalam rentang waktu yang diminta
            query = """
                SELECT 
                    sd.id,
                    sd.datetime,
                    sp.kode AS kode_sensor,
                    sp.type AS sensor_type,
                    sp.unit,
                    sd.value
                FROM sensor_petani sp
                JOIN sensor_data sd ON sp.kode = sd.kode_sensor
                WHERE sp.id_lokasi = %s AND sd.datetime >= NOW() - %s::interval
                ORDER BY sd.datetime ASC
            """
            await cur.execute(query, (id_lokasi, interval_sql))
            rows = await cur.fetchall()

            # 4. Kelompokkan data time-series per tipe sensor untuk kebutuhan grafik
            sensor_series = {
                "SOIL_MOISTURE": [],
                "SOIL_TEMP": [],
                "PH": [],
                "EC": [],
                "AIR_MOISTURE": [],
                "AIR_TEMP": [],
                "NITROGEN": [],
                "PHOSPHORUS": [],
                "POTASSIUM": []
            }

            for row in rows:
                stype = row["sensor_type"]
                if stype in sensor_series:
                    sensor_series[stype].append({
                        "datetime": row["datetime"].isoformat(),
                        "value": row["value"],
                        "unit": row["unit"] or ""
                    })

            # 5. Nilai ringkasan skor kesehatan lahan (Plant Health)
            health_score = 89
            score_delta = "Tanaman tumbuh subur dan menunjukkan kesehatan yang sangat baik."

            return {
                "message": "Berhasil mengambil data analitik dan grafik",
                "range": range,
                "data": {
                    "healthScore": health_score,
                    "scoreDelta": score_delta,
                    "sensorSeries": sensor_series,
                    "sensors": {
                        "kelembapanTanah": {"value": "45%", "status": "Aman"},
                        "suhuTanah": {"value": "28.5°C", "status": "Aman"},
                        "phTanah": {"value": "6.2", "status": "Stabil"},
                        "konduktivitas": {"value": "1.6 mS/cm", "status": "Perlu Perhatian"},
                        "kelembapanUdara": {"value": "70%", "status": "Optimal"},
                        "suhuUdara": {"value": "30.0°C", "status": "Aman"},
                        "npk": {"nitrogen": "95 ppm", "fosfor": "35 ppm", "kalium": "150 ppm"}
                    }
                }
            }