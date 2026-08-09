-- ==============================================================================
-- PROYECTO: Data Warehouse PyME de Ropa
-- SCRIPT: Creación de esquema analítico (Star Schema)
-- ==============================================================================

CREATE SCHEMA IF NOT EXISTS dw;
SET search_path TO dw;

-- ------------------------------------------------------------------------------
-- 1. CREACIÓN DE DIMENSIONES
-- ------------------------------------------------------------------------------

CREATE TABLE Dim_Producto (
    id_producto SERIAL PRIMARY KEY,
    producto_id_origen INT,
    nombre VARCHAR(150),
    categoria VARCHAR(100),
    talle VARCHAR(50),
    color VARCHAR(50),
    sku VARCHAR(50),
    precio_lista NUMERIC(12,2),
    fecha_alta DATE, 
    activo BOOLEAN
);

CREATE TABLE Dim_Tiempo (
    id_tiempo INT PRIMARY KEY, -- Formato YYYYMMDD (ej: 20260119)
    fecha DATE,
    dia INT,
    mes INT,
    nombre_mes VARCHAR(20),
    trimestre INT,
    anio INT,
    dia_semana VARCHAR(20)
);

CREATE TABLE Dim_Cliente (
    id_cliente SERIAL PRIMARY KEY,
    cliente_id_origen INT,
    nombre VARCHAR(100),
    apellido VARCHAR(100),
    genero VARCHAR(20),
    fecha_nac DATE,
    localidad VARCHAR(100),
    provincia VARCHAR(100),
    fecha_alta DATE
);

CREATE TABLE Dim_Sucursal (
    id_sucursal SERIAL PRIMARY KEY,
    sucursal_id_origen INT,
    nombre VARCHAR(50),
    direccion VARCHAR(100),
    localidad VARCHAR(50),
    provincia VARCHAR(50)
);

CREATE TABLE Dim_Empleado (
    id_empleado SERIAL PRIMARY KEY,
    empleado_id_origen INT,
    nombre VARCHAR(100),
    apellido VARCHAR(100),
    cargo VARCHAR(50),
    fecha_ingreso DATE
);

CREATE TABLE Dim_Promocion (
    id_promocion SERIAL PRIMARY KEY,
    promocion_id_origen INT,
    nombre_promocion VARCHAR(100),
    tipo_descuento VARCHAR(30),
    valor_descuento NUMERIC(5,2),
    fecha_inicio DATE,
    fecha_fin DATE,
    campania VARCHAR(50)
);

CREATE TABLE Dim_Canal (
    id_canal SERIAL PRIMARY KEY,
    canal_venta VARCHAR(50)
);

CREATE TABLE Dim_Metodo_Pago (
    id_metodo_pago SERIAL PRIMARY KEY,
    metodo_pago VARCHAR(50)
);

-- ------------------------------------------------------------------------------
-- 2. CREACIÓN DE TABLA DE HECHOS (FACT TABLE)
-- ------------------------------------------------------------------------------

CREATE TABLE Fact_Ventas (
    id_fact_venta BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    
    -- Claves Foráneas (Dimensiones)
    id_producto INT NOT NULL,
    id_cliente INT NOT NULL,
    id_tiempo INT NOT NULL,
    id_sucursal INT NOT NULL,
    id_empleado INT NOT NULL,
    id_promocion INT, -- Puede ser NULL si no hubo promoción
    id_canal INT NOT NULL,
    id_metodo_pago INT NOT NULL,
    
    -- Dimensión Degenerada y Atributos Desnormalizados
    nro_ticket VARCHAR(50) NOT NULL,
    fecha DATE, -- Desnormalizada para agilizar ciertas consultas en Power BI
    
    -- Medidas
    cantidad INT NOT NULL,
    precio_unitario_cobrado NUMERIC(12,2) NOT NULL,
    costo_historico NUMERIC(12,2) NOT NULL,
    subtotal NUMERIC(15,2) NOT NULL,

    -- Restricciones de Integridad Referencial
    CONSTRAINT fk_ventas_producto FOREIGN KEY (id_producto) REFERENCES dw.Dim_Producto(id_producto),
    CONSTRAINT fk_ventas_cliente FOREIGN KEY (id_cliente) REFERENCES dw.Dim_Cliente(id_cliente),
    CONSTRAINT fk_ventas_tiempo FOREIGN KEY (id_tiempo) REFERENCES dw.Dim_Tiempo(id_tiempo),
    CONSTRAINT fk_ventas_sucursal FOREIGN KEY (id_sucursal) REFERENCES dw.Dim_Sucursal(id_sucursal),
    CONSTRAINT fk_ventas_empleado FOREIGN KEY (id_empleado) REFERENCES dw.Dim_Empleado(id_empleado),
    CONSTRAINT fk_ventas_promocion FOREIGN KEY (id_promocion) REFERENCES dw.Dim_Promocion(id_promocion),
    CONSTRAINT fk_ventas_canal FOREIGN KEY (id_canal) REFERENCES dw.Dim_Canal(id_canal),
    CONSTRAINT fk_ventas_metodo_pago FOREIGN KEY (id_metodo_pago) REFERENCES dw.Dim_Metodo_Pago(id_metodo_pago)
);

-- ------------------------------------------------------------------------------
-- 3. POBLADO DE DIMENSIONES ESTÁTICAS Y TIEMPO
-- ------------------------------------------------------------------------------

-- Generación automática de Dim_Tiempo (2023 a 2027)
INSERT INTO Dim_Tiempo (id_tiempo, fecha, dia, mes, nombre_mes, trimestre, anio, dia_semana)
SELECT 
    to_char(fecha, 'YYYYMMDD')::INT AS id_tiempo,
    fecha AS fecha,
    extract(day FROM fecha) AS dia,
    extract(month FROM fecha) AS mes,
    to_char(fecha, 'Month') AS nombre_mes,
    extract(quarter FROM fecha) AS trimestre,
    extract(year FROM fecha) AS año,
    to_char(fecha, 'Day') AS dia_semana
FROM generate_series(
    '2023-01-01'::DATE, 
    '2027-12-31'::DATE, 
    '1 day'::interval
) AS fecha;

-- Las tablas Dim_Canal y Dim_Metodo_pago deberían poblarse desde Python, 
-- pero si tu estrategia es cargarlas en este paso, aquí está el SQL:
INSERT INTO dw.Dim_Metodo_Pago (metodo_pago)
SELECT nombre FROM public.metodos_pago;

INSERT INTO dw.Dim_Canal (canal_venta)
SELECT DISTINCT canal_venta FROM public.ventas_cabecera;