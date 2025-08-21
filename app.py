from fastapi import FastAPI
from fastapi.responses import JSONResponse
import mysql.connector
from mysql.connector import pooling, Error
from datetime import datetime
import os
import uvicorn
from collections import defaultdict


#  CONFIGURACIÓN DB

DB_CONFIG = {
    "host": os.getenv("DB_HOST", "mdb-test.c6vunyturrl6.us-west-1.rds.amazonaws.com"),
    "user": os.getenv("DB_USER", "postulaciones"),
    "password": os.getenv("DB_PASS", "post123456"),
    "database": os.getenv("DB_NAME", "airline"),
    "pool_name": "airline_pool",
    "pool_size": 5,
    "pool_reset_session": True
}

try:
    connection_pool = pooling.MySQLConnectionPool(**DB_CONFIG)
except Error as e:
    print(" Error creando el pool de conexiones:", e)

def get_connection():
    try:
        return connection_pool.get_connection()
    except Error:
        return None


#  UTILIDADES

def to_camel_case(snake_str: str) -> str:
    parts = snake_str.split('_')
    return parts[0] + ''.join(word.capitalize() for word in parts[1:])

def dict_to_camel(d: dict) -> dict:
    return {to_camel_case(k): v for k, v in d.items()}

def to_unix(dt):
    if isinstance(dt, datetime):
        return int(dt.timestamp())
    return dt


#  ALGORITMO DE ASIGNACIÓN DE ASIENTOS
def assign_seats(passengers, airplane_id):
    """
    Asigna asientos a boarding_pass sin seat_id siguiendo las reglas de negocio.
    """
    conn = get_connection()
    if not conn:
        return passengers
    
    try:
        cursor = conn.cursor(dictionary=True)

        # Traer todos los asientos del avión
        cursor.execute("""
            SELECT s.seat_id, s.seat_column, s.seat_row, s.seat_type_id
            FROM seat s
            WHERE s.airplane_id = %s
            ORDER BY s.seat_row, s.seat_column
        """, (airplane_id,))
        all_seats = cursor.fetchall()

        #  Mapear asientos por clase
        seats_by_type = defaultdict(list)
        for s in all_seats:
            seats_by_type[s["seat_type_id"]].append(s)

        #  Marcar asientos ocupados
        occupied = {p["seatId"] for p in passengers if p["seatId"]}
        
        #  Agrupar pasajeros por compra
        groups = defaultdict(list)
        for p in passengers:
            groups[p["purchaseId"]].append(p)

        # Intentar asignar asientos grupo por grupo
        for purchase_id, group in groups.items():
            # Separar los que ya tienen asiento
            already_seated = [p for p in group if p["seatId"]]
            to_assign = [p for p in group if not p["seatId"]]

            if not to_assign:
                continue

            # Usar el seat_type_id del primer pasajero del grupo
            seat_type = to_assign[0]["seatTypeId"]

            # Filtrar asientos libres de esa clase
            available = [s for s in seats_by_type[seat_type] if s["seat_id"] not in occupied]

            # Asignar en orden, procurando cercanía (simplemente de forma secuencial)
            for idx, p in enumerate(to_assign):
                if idx < len(available):
                    chosen = available[idx]
                    p["seatId"] = chosen["seat_id"]
                    occupied.add(chosen["seat_id"])

        return passengers

    finally:
        if conn.is_connected():
            cursor.close()
            conn.close()


#  APP FASTAPI
app = FastAPI(title="Andes Airlines Check-in API")


# ENDPOINT PRINCIPAL

@app.get("/flights/{flight_id}/passengers")
def get_passengers(flight_id: int):
    conn = get_connection()
    if not conn:
        return JSONResponse(
            status_code=400, 
            content={"code": 400, "errors": "could not connect to db"}
        )
    
    try:
        cursor = conn.cursor(dictionary=True)

        # Buscar vuelo
        cursor.execute("SELECT * FROM flight WHERE flight_id = %s", (flight_id,))
        flight = cursor.fetchone()
        if not flight:
            return {"code": 404, "data": {}}

        # Buscar pasajeros y boarding_pass
        cursor.execute("""
            SELECT 
                p.passenger_id,
                p.dni,
                p.name,
                p.age,
                p.country,
                bp.boarding_pass_id,
                bp.purchase_id,
                bp.seat_type_id,
                bp.seat_id
            FROM passenger p
            JOIN boarding_pass bp ON bp.passenger_id = p.passenger_id
            WHERE bp.flight_id = %s
        """, (flight_id,))
        passengers = cursor.fetchall()

        # Convertir a camelCase
        passengers = [dict_to_camel(p) for p in passengers]

        # Asignar asientos a los que no tienen
        passengers = assign_seats(passengers, flight["airplane_id"])

        # Construcción de respuesta
        data = {
            "flightId": flight["flight_id"],
            "takeoffDateTime": to_unix(flight.get("takeoff_date_time")),
            "takeoffAirport": flight.get("takeoff_airport"),
            "landingDateTime": to_unix(flight.get("landing_date_time")),
            "landingAirport": flight.get("landing_airport"),
            "airplaneId": flight.get("airplane_id"),
            "passengers": passengers
        }

        return {"code": 200, "data": data}

    except Exception as e:
        return JSONResponse(
            status_code=400, 
            content={"code": 400, "errors": str(e)}
        )
    finally:
        if conn.is_connected():
            cursor.close()
            conn.close()


#  EJECUCIÓN LOCAL
if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)
