


create table provincias(

	id INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
	nombre varchar(100) not null constraint nombre_provincia_unico UNIQUE
);

create table localidades(
	id INT generated always as identity primary key,
	nombre varchar(100) not null,
	provincia_id int not null,
	constraint fk_localidad_provincia
		foreign key (provincia_id)
		references provincias(id)
);

create table categorias(

	id int generated always as identity primary key,
	nombre varchar(100) not null unique
);

create table clientes(
	id int generated always as identity primary key,
	nombre varchar(100) not null,
	apellido varchar(100) not null,
	dni varchar(20) not null unique,
	email varchar(150) unique,
	fecha_nacimiento date,
	genero varchar(20),
	localidad_id int not null,
	fecha_alta timestamp default current_timestamp,
	constraint fk_clientes_localidad
		foreign key (localidad_id) references localidades(id),
	constraint check_genero 
		check (genero in('Masculino','Femenino','otro' ))

);


create table sucursales(
	id int generated always as identity primary key,
	nombre varchar(100) not null,
	direccion varchar(255),
	localidad_id int not null,
	constraint fk_sucursal_localidad
		foreign key (localidad_id) references localidades(id)
);

create table empleados(
	id int generated always as identity primary key,
	nombre varchar(100) not null,
	apellido varchar(100) not null,
	sucursal_id int not null,
	cargo varchar(50),
	fecha_ingreso date default current_date,
	constraint fk_empleado_sucursal
		foreign key (sucursal_id) references sucursales(id)
	
);

create table productos(
	id int generated always as identity primary key,
	categoria_id int not null,
	nombre varchar(150) not null,
	descripcion text,
	precio_lista numeric(12,2) not null check (precio_lista>=0),
	constraint fk_producto_categoria
		foreign key (categoria_id) references categorias(id)
);

create table talles(
	id int generated always as identity primary key,
	descripcion varchar(50) not null
);

create table colores(
	id int generated always as identity primary key,
	descripcion varchar(50) not null,
	codigo_hex char(7) check (codigo_hex ~ '^#[A-Fa-f0-9]{6}$')
	
);
create table variantes(
	id int generated always as identity primary key,
	producto_id int not null,
	talle_id int not null,
	color_id int not null,
	sku varchar(50) unique not null, --codigo de barras
	stock_minimo int default 0 check (stock_minimo >=0),
	constraint fk_variante_producto foreign key (producto_id) references productos(id),
	constraint fk_variante_talle foreign key (talle_id) references talles(id),
	constraint fk_variante_color foreign key (color_id) references colores(id)

);

create table metodos_pago(
	id int generated always as identity primary key,
	nombre varchar(50) not null unique
);

create table campanias_marketing(
 	id int generated always as identity primary key,
 	nombre varchar(100) not null,
	fecha_inicio date not null,
	fecha_fin date not null,
	presupuesto numeric(12,2) default 0
);

create table ventas_cabecera(
	id int generated always as identity primary key,
	fecha_hora timestamp default current_timestamp,
	cliente_id int not null,
	sucursal_id int not null,
	empleado_id int not null,
	metodo_pago_id int not null,
	campania_id int,
	total_venta numeric(12,2) not null default 0 check (total_venta>=0),

	constraint fk_venta_cliente foreign key (cliente_id) references clientes(id),
	constraint fk_venta_sucursal foreign key (sucursal_id) references sucursales(id),
	constraint fk_venta_empleado foreign key (empleado_id) references empleados(id),
	constraint fk_venta_pago foreign key (metodo_pago_id) references metodos_pago(id),
	constraint fk_venta_campania foreign key (campania_id) references campanias_marketing(id)
);

create table ventas_detalle (

	id int generated always as identity primary key,
	venta_id int not null,
	variante_id INT NOT NULL,
    cantidad INT NOT NULL CHECK (cantidad > 0),
    precio_unitario_cobrado NUMERIC(12,2) NOT NULL CHECK (precio_unitario_cobrado >= 0),
    costo_unitario_historico NUMERIC(12,2) NOT NULL CHECK (costo_unitario_historico >= 0),
    subtotal NUMERIC(12,2) NOT NULL, -- cantidad * precio_unitario_cobrado
	CONSTRAINT fk_detalle_venta FOREIGN KEY (venta_id) REFERENCES ventas_cabecera(id) ON DELETE CASCADE,
    CONSTRAINT fk_detalle_variante FOREIGN KEY (variante_id) REFERENCES variantes(id)
);

create table proveedores(
	id int generated always as identity primary key,
	razon_social varchar(150) not null,
	cuit varchar(20) unique not null,
	email varchar(150),
	localidad_id int not null,
	constraint fk_proveedor_localidad foreign key (localidad_id) references localidades(id)
);

create table ordenes_compra_cabecera(
	id int generated always as identity primary key,
	proveedor_id int not null,
	fecha_pedido date default current_date,
	estado_pedido varchar(50) default 'Pendiente',
	total_compra numeric(12,2) default 0,
	constraint fk_oc_proveedor foreign key (proveedor_id) references proveedores(id),
	CONSTRAINT check_estado_compra CHECK (estado_pedido IN ('Pendiente', 'Recibido', 'Cancelado'))
);

create table ordenes_compra_detalle(
  	id int generated always as identity primary key,
	orden_compra_id int not null,
	variante_id int not null,
	cantidad int not null check (cantidad>0),
	costo_unitario_pactado NUMERIC(12,2) NOT NULL CHECK (costo_unitario_pactado >= 0),
    CONSTRAINT fk_oc_detalle_cabecera FOREIGN KEY (orden_compra_id) REFERENCES ordenes_compra_cabecera(id) ON DELETE CASCADE,
    CONSTRAINT fk_oc_detalle_variante FOREIGN KEY (variante_id) REFERENCES variantes(id)
);
create table promociones(
id INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    nombre VARCHAR(100) NOT NULL,
    descripcion TEXT,
    tipo_descuento VARCHAR(50) CHECK (tipo_descuento IN ('Porcentaje', 'Monto Fijo')),
    valor_descuento NUMERIC(12,2) NOT NULL,
    fecha_inicio DATE NOT NULL,
    fecha_fin DATE NOT NULL,
    CONSTRAINT check_fechas_promo CHECK (fecha_fin >= fecha_inicio)
);

ALTER TABLE ventas_detalle 
ADD COLUMN promocion_id INT,
ADD CONSTRAINT fk_detalle_promocion 
    FOREIGN KEY (promocion_id) REFERENCES promociones(id);

	CREATE TABLE campanias_promociones (
    campania_id INT NOT NULL,
    promocion_id INT NOT NULL,

    PRIMARY KEY (campania_id, promocion_id),

    CONSTRAINT fk_cp_campania
        FOREIGN KEY (campania_id)
        REFERENCES campanias_marketing(id),

    CONSTRAINT fk_cp_promocion
        FOREIGN KEY (promocion_id)
        REFERENCES promociones(id)
);

ALTER TABLE productos ADD COLUMN fecha_alta date DEFAULT CURRENT_DATE;
ALTER TABLE productos ADD COLUMN activo boolean DEFAULT true;



ALTER TABLE ventas_cabecera 
ADD COLUMN canal_venta VARCHAR(50) DEFAULT 'Presencial';

ALTER TABLE ventas_cabecera 
ADD CONSTRAINT check_canal_venta 
CHECK (canal_venta IN ('Presencial', 'Web'));

-- insertar 
INSERT INTO colores (descripcion, codigo_hex) VALUES
('Negro', '#000000'),
('Blanco', '#FFFFFF'),
('Rojo', '#FF0000'),
('Azul', '#0000FF'),
('Verde', '#00FF00'),
('Amarillo', '#FFFF00'),
('Naranja', '#FFA500'),
('Violeta', '#8B00FF'),
('Rosa', '#FFC0CB'),
('Celeste', '#87CEEB'),
('Turquesa', '#40E0D0'),
('Coral', '#FF7F50'),
('Lavanda', '#E6E6FA'),
('Menta', '#98FF98'),
('Gris', '#808080'),
('Beige', '#F5F5DC'),
('Marrón', '#8B4513'),
('Bordó', '#800020'),
('Fucsia', '#FF00FF'),
('Índigo', '#4B0082');

INSERT INTO talles (descripcion) VALUES 
('S'),
('M'),
('L'),
('XL'),
('XXL'),
('unico');

INSERT INTO categorias (nombre) VALUES 
('Remeras'), ('Pantalones'), ('Abrigos'), ('Accesorios');

INSERT INTO metodos_pago (nombre) VALUES 
('Efectivo'), 
('Transferencia Bancaria'), 
('Tarjeta de Débito'), 
('Tarjeta de Crédito'), 
('Mercado Pago');

INSERT INTO promociones (nombre, tipo_descuento, valor_descuento, fecha_inicio, fecha_fin) VALUES 
('Black Friday', 'Porcentaje', 20.00, '2024-11-20', '2024-11-30'),
('Descuento fiestas', 'Monto Fijo', 1000.00, '2025-12-01', '2025-12-31'),
('Descuento Aper', 'Monto Fijo', 1000.00, '2025-01-01', '2025-12-31');


INSERT INTO proveedores
(razon_social, cuit, email, localidad_id, direccion, telefono, condicion_pago, categoria)
VALUES
('Moda Norte SRL', '30-70111222-3', 'ventas@modanorte.com.ar', 2, 'San Martín 456', '11-4321-9876', 'Contado', 'Accesorios'),
('Calzados Río SA', '33-70999888-1', 'ventas@calzadosrio.com.ar', 3, 'Belgrano 890', '11-4999-1122', '60 días', 'Calzado'),
('Insumos Urbanos', '30-74555666-9', 'ventas@insumosurbanos.com.ar', 1, 'Mitre 250', '11-4555-6677', '90 días', 'Insumos'),
('Textiles del Litoral S.A.', '30-71234567-8', 'ventas@textilesdellitoral.com.ar', 1, 'Av. Independencia 1234', '11-4567-8901', '30 días', 'Textiles')
ON CONFLICT (cuit) DO NOTHING;


INSERT INTO campanias_marketing
(nombre, fecha_inicio, fecha_fin, presupuesto)
VALUES
('Hot Sale 2025', '2025-05-20', '2025-05-24', 800000),
('Cyber Monday 2025', '2025-11-25', '2025-11-27', 900000),
('Black Friday 2025', '2025-11-20', '2025-11-30', 1200000),
('Campaña Verano', '2025-01-01', '2025-03-31', 1500000),
('Día del Padre', '2025-06-01', '2025-06-18', 450000);

INSERT INTO campanias_promociones (campania_id, promocion_id)
VALUES
(1, 5),  -- Hot Sale → Promo Hot Sale
(2, 4),  -- Cyber Monday → Promo Cyber Monday
(3, 1),  -- Black Friday → Promo Black Friday
(4, 7),  -- Campaña Verano → Liquidación Verano
(4, 9),  -- Campaña Verano → Preventa Verano
(5, 3);  -- Día del Padre → Promo Día del Padre

INSERT INTO sucursales (nombre, direccion, localidad_id) VALUES
('Centro', 'Av. Principal 123', 2193),           -- id: 1
('Norte', 'Calle Norte 456', 2193),              -- id: 2
('Shopping', 'Shopping Mall Local 789', 1038),   -- id: 3
('Tienda Online', 'Oficina Central', 2193); 

INSERT INTO empleados (nombre, apellido, sucursal_id, cargo, fecha_ingreso) VALUES
('Juan', 'Pérez', 1, 'Vendedor', '2023-12-1'),
('María', 'González', 1, 'Cajero', '2023-04-20'),
('Carlos', 'Rodríguez', 1, 'Supervisor', '2023-11-10'),

-- Empleados de sucursal Norte (id: 2)
('Ana', 'Martínez', 2, 'Vendedor', '2023-02-01'),
('Luis', 'Fernández', 2, 'Cajero', '2023-05-12'),
('Laura', 'López', 2, 'Vendedor', '2023-04-08'),

-- Empleados de sucursal Shopping (id: 3)
('Diego', 'Sánchez', 3, 'Vendedor', '2023-06-15'),
('Sofía', 'Ramírez', 3, 'Supervisor', '2023-12-05'),

-- Empleados de Tienda Online (id: 4)
('Pablo', 'Torres', 4, 'Operador Web', '2023-07-01'),
('Valentina', 'Díaz', 4, 'Soporte Online', '2023-08-10');

TRUNCATE TABLE empleados RESTART IDENTITY CASCADE;

TRUNCATE TABLE sucursales RESTART IDENTITY CASCADE;

select * from colores;
select * from metodos_pago;
select * from categorias;
SELECT constraint_name, constraint_type 
FROM information_schema.table_constraints 
WHERE table_name = 'provincias';
select * from proveedores;
select * from sucursales;
select * from empleados;
select * from provincias;	
select * from localidades;
select * from localidades where nombre='Resistencia';
select * from talles;
select*from campanias_marketing;

select * from clientes;
select * from productos;
select * from variantes;
select * from ordenes_compra_detalle;
select * from ordenes_compra_cabecera;
select * from ventas_cabecera;
select * from ventas_detalle;
select * from promociones;
SELECT * FROM clientes WHERE dni = 32899636;
select * from ventas_cabecera where canal_venta='Web'

select * from ventas_cabecera where campania_id=4

