-- ==============================================================================
-- PROYECTO: Sistema de Ventas - PyME de Ropa
-- SCRIPT: Creación de esquema transaccional (Origen de datos) y Datos Maestros
-- ==============================================================================

-- ------------------------------------------------------------------------------
-- 1. CREACIÓN DE TABLAS (DDL)
-- ------------------------------------------------------------------------------

CREATE TABLE provincias (
    id INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    nombre VARCHAR(100) NOT NULL CONSTRAINT nombre_provincia_unico UNIQUE
);

CREATE TABLE localidades (
    id INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    nombre VARCHAR(100) NOT NULL,
    provincia_id INT NOT NULL,
    CONSTRAINT fk_localidad_provincia FOREIGN KEY (provincia_id) REFERENCES provincias(id)
);

CREATE TABLE categorias (
    id INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    nombre VARCHAR(100) NOT NULL UNIQUE
);

CREATE TABLE clientes (
    id INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    nombre VARCHAR(100) NOT NULL,
    apellido VARCHAR(100) NOT NULL,
    dni VARCHAR(20) NOT NULL UNIQUE,
    email VARCHAR(150) UNIQUE,
    fecha_nacimiento DATE,
    genero VARCHAR(20),
    localidad_id INT NOT NULL,
    fecha_alta TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT fk_clientes_localidad FOREIGN KEY (localidad_id) REFERENCES localidades(id),
    CONSTRAINT check_genero CHECK (genero IN ('Masculino', 'Femenino', 'otro'))
);

CREATE TABLE sucursales (
    id INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    nombre VARCHAR(100) NOT NULL,
    direccion VARCHAR(255),
    localidad_id INT NOT NULL,
    CONSTRAINT fk_sucursal_localidad FOREIGN KEY (localidad_id) REFERENCES localidades(id)
);

CREATE TABLE empleados (
    id INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    nombre VARCHAR(100) NOT NULL,
    apellido VARCHAR(100) NOT NULL,
    sucursal_id INT NOT NULL,
    cargo VARCHAR(50),
    fecha_ingreso DATE DEFAULT CURRENT_DATE,
    CONSTRAINT fk_empleado_sucursal FOREIGN KEY (sucursal_id) REFERENCES sucursales(id)
);

CREATE TABLE productos (
    id INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    categoria_id INT NOT NULL,
    nombre VARCHAR(150) NOT NULL,
    descripcion TEXT,
    precio_lista NUMERIC(12,2) NOT NULL CHECK (precio_lista >= 0),
    fecha_alta DATE DEFAULT CURRENT_DATE,
    activo BOOLEAN DEFAULT true,
    CONSTRAINT fk_producto_categoria FOREIGN KEY (categoria_id) REFERENCES categorias(id)
);

CREATE TABLE talles (
    id INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    descripcion VARCHAR(50) NOT NULL
);

CREATE TABLE colores (
    id INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    descripcion VARCHAR(50) NOT NULL,
    codigo_hex CHAR(7) CHECK (codigo_hex ~ '^#[A-Fa-f0-9]{6}$')
);

CREATE TABLE variantes (
    id INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    producto_id INT NOT NULL,
    talle_id INT NOT NULL,
    color_id INT NOT NULL,
    sku VARCHAR(50) UNIQUE NOT NULL, -- codigo de barras
    stock_minimo INT DEFAULT 0 CHECK (stock_minimo >= 0),
    activo BOOLEAN DEFAULT true,
    CONSTRAINT fk_variante_producto FOREIGN KEY (producto_id) REFERENCES productos(id),
    CONSTRAINT fk_variante_talle FOREIGN KEY (talle_id) REFERENCES talles(id),
    CONSTRAINT fk_variante_color FOREIGN KEY (color_id) REFERENCES colores(id)
);

CREATE TABLE metodos_pago (
    id INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    nombre VARCHAR(50) NOT NULL UNIQUE
);

CREATE TABLE campanias_marketing (
    id INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    nombre VARCHAR(100) NOT NULL,
    fecha_inicio DATE NOT NULL,
    fecha_fin DATE NOT NULL,
    presupuesto NUMERIC(12,2) DEFAULT 0
);

CREATE TABLE promociones (
    id INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    nombre VARCHAR(100) NOT NULL,
    descripcion TEXT,
    tipo_descuento VARCHAR(50) CHECK (tipo_descuento IN ('Porcentaje', 'Monto Fijo')),
    valor_descuento NUMERIC(12,2) NOT NULL,
    fecha_inicio DATE NOT NULL,
    fecha_fin DATE NOT NULL,
    CONSTRAINT check_fechas_promo CHECK (fecha_fin >= fecha_inicio)
);

CREATE TABLE campanias_promociones (
    campania_id INT NOT NULL,
    promocion_id INT NOT NULL,
    PRIMARY KEY (campania_id, promocion_id),
    CONSTRAINT fk_cp_campania FOREIGN KEY (campania_id) REFERENCES campanias_marketing(id),
    CONSTRAINT fk_cp_promocion FOREIGN KEY (promocion_id) REFERENCES promociones(id)
);

CREATE TABLE ventas_cabecera (
    id INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    fecha_hora TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    cliente_id INT NOT NULL,
    sucursal_id INT NOT NULL,
    empleado_id INT NOT NULL,
    metodo_pago_id INT NOT NULL,
    campania_id INT,
    canal_venta VARCHAR(50) DEFAULT 'Presencial' CHECK (canal_venta IN ('Presencial', 'Web')),
    total_venta NUMERIC(12,2) NOT NULL DEFAULT 0 CHECK (total_venta >= 0),
    CONSTRAINT fk_venta_cliente FOREIGN KEY (cliente_id) REFERENCES clientes(id),
    CONSTRAINT fk_venta_sucursal FOREIGN KEY (sucursal_id) REFERENCES sucursales(id),
    CONSTRAINT fk_venta_empleado FOREIGN KEY (empleado_id) REFERENCES empleados(id),
    CONSTRAINT fk_venta_pago FOREIGN KEY (metodo_pago_id) REFERENCES metodos_pago(id),
    CONSTRAINT fk_venta_campania FOREIGN KEY (campania_id) REFERENCES campanias_marketing(id)
);

CREATE TABLE ventas_detalle (
    id INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    venta_id INT NOT NULL,
    variante_id INT NOT NULL,
    promocion_id INT,
    cantidad INT NOT NULL CHECK (cantidad > 0),
    precio_unitario_cobrado NUMERIC(12,2) NOT NULL CHECK (precio_unitario_cobrado >= 0),
    costo_unitario_historico NUMERIC(12,2) NOT NULL CHECK (costo_unitario_historico >= 0),
    subtotal NUMERIC(12,2) NOT NULL, -- cantidad * precio_unitario_cobrado
    CONSTRAINT fk_detalle_venta FOREIGN KEY (venta_id) REFERENCES ventas_cabecera(id) ON DELETE CASCADE,
    CONSTRAINT fk_detalle_variante FOREIGN KEY (variante_id) REFERENCES variantes(id),
    CONSTRAINT fk_detalle_promocion FOREIGN KEY (promocion_id) REFERENCES promociones(id)
);

CREATE TABLE proveedores (
    id INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    razon_social VARCHAR(150) NOT NULL,
    cuit VARCHAR(20) UNIQUE NOT NULL,
    email VARCHAR(150),
    localidad_id INT NOT NULL,
    direccion VARCHAR(255),
    telefono VARCHAR(50),
    condicion_pago VARCHAR(50),
    categoria VARCHAR(100),
    CONSTRAINT fk_proveedor_localidad FOREIGN KEY (localidad_id) REFERENCES localidades(id)
);

CREATE TABLE ordenes_compra_cabecera (
    id INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    proveedor_id INT NOT NULL,
    fecha_pedido DATE DEFAULT CURRENT_DATE,
    estado_pedido VARCHAR(50) DEFAULT 'Pendiente',
    total_compra NUMERIC(12,2) DEFAULT 0,
    CONSTRAINT fk_oc_proveedor FOREIGN KEY (proveedor_id) REFERENCES proveedores(id),
    CONSTRAINT check_estado_compra CHECK (estado_pedido IN ('Pendiente', 'Recibido', 'Cancelado'))
);

CREATE TABLE ordenes_compra_detalle (
    id INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    orden_compra_id INT NOT NULL,
    variante_id INT NOT NULL,
    cantidad INT NOT NULL CHECK (cantidad > 0),
    costo_unitario_pactado NUMERIC(12,2) NOT NULL CHECK (costo_unitario_pactado >= 0),
    CONSTRAINT fk_oc_detalle_cabecera FOREIGN KEY (orden_compra_id) REFERENCES ordenes_compra_cabecera(id) ON DELETE CASCADE,
    CONSTRAINT fk_oc_detalle_variante FOREIGN KEY (variante_id) REFERENCES variantes(id)
);


-- ------------------------------------------------------------------------------
-- 2. INSERCIÓN DE DATOS MAESTROS Y PARÁMETROS (DML)
-- ------------------------------------------------------------------------------

-- Ubicaciones Básicas (IDs 1 y 2 generados automáticamente)
INSERT INTO provincias (nombre) VALUES 
('Chaco'), 
('Corrientes');

INSERT INTO localidades (nombre, provincia_id) VALUES 
('Resistencia', 1),  -- ID 1
('Corrientes', 2),   -- ID 2
('Barranqueras', 1); -- ID 3

INSERT INTO colores (descripcion, codigo_hex) VALUES
('Negro', '#000000'), ('Blanco', '#FFFFFF'), ('Rojo', '#FF0000'),
('Azul', '#0000FF'), ('Verde', '#00FF00'), ('Amarillo', '#FFFF00'),
('Naranja', '#FFA500'), ('Violeta', '#8B00FF'), ('Rosa', '#FFC0CB'),
('Celeste', '#87CEEB'), ('Turquesa', '#40E0D0'), ('Coral', '#FF7F50'),
('Lavanda', '#E6E6FA'), ('Menta', '#98FF98'), ('Gris', '#808080'),
('Beige', '#F5F5DC'), ('Marrón', '#8B4513'), ('Bordó', '#800020'),
('Fucsia', '#FF00FF'), ('Índigo', '#4B0082');

INSERT INTO talles (descripcion) VALUES 
('S'), ('M'), ('L'), ('XL'), ('XXL'), ('unico');

INSERT INTO categorias (nombre) VALUES 
('Remeras'), ('Pantalones'), ('Abrigos'), ('Accesorios');

INSERT INTO metodos_pago (nombre) VALUES 
('Efectivo'), ('Transferencia Bancaria'), ('Tarjeta de Débito'), 
('Tarjeta de Crédito'), ('Mercado Pago');

-- Proveedores (Usando localidades creadas arriba: 1, 2 y 3)
INSERT INTO proveedores (razon_social, cuit, email, localidad_id, direccion, telefono, condicion_pago, categoria) VALUES
('Moda Norte SRL', '30-70111222-3', 'ventas@modanorte.com.ar', 2, 'San Martín 456', '11-4321-9876', 'Contado', 'Accesorios'),
('Calzados Río SA', '33-70999888-1', 'ventas@calzadosrio.com.ar', 3, 'Belgrano 890', '11-4999-1122', '60 días', 'Calzado'),
('Insumos Urbanos', '30-74555666-9', 'ventas@insumosurbanos.com.ar', 1, 'Mitre 250', '11-4555-6677', '90 días', 'Insumos'),
('Textiles del Litoral S.A.', '30-71234567-8', 'ventas@textilesdellitoral.com.ar', 1, 'Av. Independencia 1234', '11-4567-8901', '30 días', 'Textiles')
ON CONFLICT (cuit) DO NOTHING;

-- Campañas
INSERT INTO campanias_marketing (nombre, fecha_inicio, fecha_fin, presupuesto) VALUES
('Hot Sale 2025', '2025-05-20', '2025-05-24', 800000),
('Cyber Monday 2025', '2025-11-25', '2025-11-27', 900000),
('Black Friday 2025', '2025-11-20', '2025-11-30', 1200000),
('Campaña Verano', '2025-01-01', '2025-03-31', 1500000),
('Día del Padre', '2025-06-01', '2025-06-18', 450000);

-- Promociones (Completadas para que el cruce posterior funcione)
INSERT INTO promociones (nombre, tipo_descuento, valor_descuento, fecha_inicio, fecha_fin) VALUES 
('Black Friday', 'Porcentaje', 20.00, '2025-11-20', '2025-11-30'), -- ID 1
('Descuento fiestas', 'Monto Fijo', 1000.00, '2025-12-01', '2025-12-31'), -- ID 2
('Promo Día del Padre', 'Porcentaje', 15.00, '2025-06-01', '2025-06-18'), -- ID 3
('Promo Cyber Monday', 'Porcentaje', 25.00, '2025-11-25', '2025-11-27'), -- ID 4
('Promo Hot Sale', 'Porcentaje', 30.00, '2025-05-20', '2025-05-24'), -- ID 5
('Promo Genérica', 'Monto Fijo', 500.00, '2025-01-01', '2025-12-31'), -- ID 6
('Liquidación Verano', 'Porcentaje', 40.00, '2025-02-15', '2025-03-31'), -- ID 7
('Promo Otoño', 'Porcentaje', 10.00, '2025-03-21', '2025-06-20'), -- ID 8
('Preventa Verano', 'Monto Fijo', 1500.00, '2025-01-01', '2025-01-31'); -- ID 9

-- Cruce Campañas y Promociones
INSERT INTO campanias_promociones (campania_id, promocion_id) VALUES
(1, 5),  -- Hot Sale → Promo Hot Sale
(2, 4),  -- Cyber Monday → Promo Cyber Monday
(3, 1),  -- Black Friday → Promo Black Friday
(4, 7),  -- Campaña Verano → Liquidación Verano
(4, 9),  -- Campaña Verano → Preventa Verano
(5, 3);  -- Día del Padre → Promo Día del Padre

-- Sucursales (Apuntan a los IDs 1 y 2 de localidades creados al principio)
INSERT INTO sucursales (nombre, direccion, localidad_id) VALUES
('Centro', 'Av. Principal 123', 1),            -- id: 1
('Norte', 'Calle Norte 456', 2),               -- id: 2
('Shopping', 'Shopping Mall Local 789', 1),    -- id: 3
('Tienda Online', 'Oficina Central', 2);       -- id: 4

-- Empleados
INSERT INTO empleados (nombre, apellido, sucursal_id, cargo, fecha_ingreso) VALUES
('Juan', 'Pérez', 1, 'Vendedor', '2023-12-01'),
('María', 'González', 1, 'Cajero', '2023-04-20'),
('Carlos', 'Rodríguez', 1, 'Supervisor', '2023-11-10'),
('Ana', 'Martínez', 2, 'Vendedor', '2023-02-01'),
('Luis', 'Fernández', 2, 'Cajero', '2023-05-12'),
('Laura', 'López', 2, 'Vendedor', '2023-04-08'),
('Diego', 'Sánchez', 3, 'Vendedor', '2023-06-15'),
('Sofía', 'Ramírez', 3, 'Supervisor', '2023-12-05'),
('Pablo', 'Torres', 4, 'Operador Web', '2023-07-01'),
('Valentina', 'Díaz', 4, 'Soporte Online', '2023-08-10');