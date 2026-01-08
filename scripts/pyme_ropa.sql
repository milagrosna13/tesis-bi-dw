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
)

create table ordenes_compra_detalle(
  	id int generated always as identity primary key,
	orden_compra_id int not null,
	variante_id int not null,
	cantidad int not null check (cantidad>0),
	costo_unitario_pactado NUMERIC(12,2) NOT NULL CHECK (costo_unitario_pactado >= 0),
    CONSTRAINT fk_oc_detalle_cabecera FOREIGN KEY (orden_compra_id) REFERENCES ordenes_compra_cabecera(id) ON DELETE CASCADE,
    CONSTRAINT fk_oc_detalle_variante FOREIGN KEY (variante_id) REFERENCES variantes(id)
)
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

create table tipo_movimiento(
	id serial primary key,
	descripcion varchar(100) not null,
	signo smallint not null --1 para sumar (compra), -1 para restar (venta/merma)
)
--motivos de movimiento 
INSERT INTO tipo_movimiento (descripcion, signo) VALUES 
('Venta', -1),
('Compra', 1),
('Ajuste por rotura (Merma)', -1),
('Vencimiento (Merma)', -1),
('Transferencia - Salida', -1),
('Transferencia - Entrada', 1),
('Devolución de cliente', 1);

create table stock_movimiento(
	id int generated ALWAYS AS IDENTITY PRIMARY KEY,
	variante_id int not null,
	sucursal_id int not null,
	tipo_movimiento_id int not null,
	cantidad decimal (10,2) not null,
	costo_unitario decimal(10,2),
	fecha_hora timestamp default current_timestamp,
	referencia_id int, --guardar el id de la venta o compra
	observaciones text,

	constraint fk_variante foreign key (variante_id) references variantes(id),
	constraint fk_sucursal foreign key (sucursal_id) references sucursales(id),
	constraint fk_tipo_movimiento foreign key (tipo_movimiento_id) references tipo_movimiento(id)
	)


